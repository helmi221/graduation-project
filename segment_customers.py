import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timezone


cred = credentials.Certificate("serviceAccountKey.json") 
firebase_admin.initialize_app(cred)

db = firestore.client()

WEIGHTS = {
    "monetary": 0.50, 
    "frequency": 0.30,  
    "recency": 0.20     
}


def calculate_segment(total_spent, order_count, days_since_last_order):
    if order_count == 0:
        return "غير مصنف"


    if total_spent >= 5000: m_score = 5
    elif total_spent >= 3000: m_score = 4
    elif total_spent >= 1000: m_score = 3
    elif total_spent >= 500: m_score = 2
    else: m_score = 1

    if order_count >= 10: f_score = 5
    elif order_count >= 7: f_score = 4
    elif order_count >= 4: f_score = 3
    elif order_count >= 2: f_score = 2
    else: f_score = 1

    if days_since_last_order <= 30: r_score = 5
    elif days_since_last_order <= 60: r_score = 4
    elif days_since_last_order <= 90: r_score = 3
    elif days_since_last_order <= 180: r_score = 2
    else: r_score = 1

    final_score = (m_score * WEIGHTS["monetary"]) + \
                  (f_score * WEIGHTS["frequency"]) + \
                  (r_score * WEIGHTS["recency"])


    if days_since_last_order > 120 and total_spent > 2000:
        return "في خطر (استهداف فوري)" 
    if days_since_last_order > 180:
        return "خاملون"

    if final_score >= 4.5:
        return "أبطال (VIP)"
    elif final_score >= 3.5:
        return "عملاء مخلصون"
    elif final_score >= 2.5:
        return "واعدون (إنفاق عالي)"
    elif order_count == 1 and days_since_last_order <= 30:
        return "عملاء جدد"
    else:
        return "عميل عادي"


def run_segmentation():
    print(" جارٍ جلب البيانات وتحليل العملاء...")
    
    users_ref = db.collection("users").stream()
    users_data = {user.id: user.to_dict() for user in users_ref}
    
    orders_ref = db.collection("orders").stream()
    custom_ref = db.collection("custom_designs").stream()
    
    all_orders = []
    for o in orders_ref:
        data = o.to_dict()
        data['final_price'] = data.get('total', 0) 
        all_orders.append(data)
        
    for o in custom_ref:
        data = o.to_dict()
        data['final_price'] = data.get('final_price', 0)
        all_orders.append(data)

    print(f" تم العثور على {len(users_data)} مستخدم و {len(all_orders)} طلب.")
    
    batch = db.batch()
    count = 0
    
    for uid, user in users_data.items():
        user_orders = [
            o for o in all_orders 
            if o.get('customer_uid') == uid and o.get('status') == 'مكتمل'
        ]
        
        total_spent = sum(float(o.get('final_price', 0)) for o in user_orders)
        order_count = len(user_orders)
        
        days_since_last = 999
        if user_orders:
            dates = []
            for o in user_orders:
                try:
                    d = datetime.fromisoformat(o['order_date'].replace('Z', '+00:00'))
                    dates.append(d)
                except:
                    pass
            
            if dates:
                last_order_date = max(dates)
                delta = datetime.now(timezone.utc) - last_order_date
                days_since_last = delta.days

        segment = calculate_segment(total_spent, order_count, days_since_last)
        
        user_ref = db.collection("users").document(uid)
        batch.update(user_ref, {
            "customerSegment": segment,
            "lastClassified": datetime.now().isoformat(),
            "totalSpending_calculated": total_spent,
            "orderCount_calculated": order_count
        })
        
        count += 1
        if count % 400 == 0:
            batch.commit()
            batch = db.batch()
            print(f" تم تحديث دفعة من {count} عميل...")

    batch.commit()
    print(f" تم الانتهاء! تم تصنيف {count} عميل بنجاح.")

if __name__ == "__main__":
    run_segmentation()