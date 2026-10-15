#!/usr/bin/env python3
"""
Synthetic data generator for OmniRetail pipeline.
Generates product catalogs, user profile changes (SCD Type 2),
clickstream events, and transactional order data.
"""

import os
import json
import uuid
import random
import argparse
from datetime import datetime, timedelta, timezone

CATEGORIES = [
    ("Electronics", 0.15, 0.45),
    ("Apparel & Fashion", 0.40, 0.70),
    ("Groceries & Food", 0.08, 0.25),
    ("Home & Living", 0.25, 0.55),
    ("Beauty & Health", 0.35, 0.65),
    ("Sports & Outdoors", 0.20, 0.50),
]

REGIONS = ["Central", "Northwest", "South", "Volga", "Ural", "Siberia", "Far East"]
LOYALTY_TIERS = [
    ("Bronze", 0.00),
    ("Silver", 0.05),
    ("Gold", 0.10),
    ("Platinum", 0.15),
]
DEVICE_TYPES = ["ios", "android", "desktop", "mobile_web"]


def generate_catalog(num_products=50):
    products = []
    for pid in range(1, num_products + 1):
        cat, min_m, max_m = random.choice(CATEGORIES)
        base_price = round(random.uniform(15.0, 850.0), 2)
        margin_pct = random.uniform(min_m, max_m)
        cost_price = round(base_price * (1.0 - margin_pct), 2)
        
        products.append({
            "product_id": pid,
            "sku": f"SKU-{cat[:3].upper()}-{pid:05d}",
            "title": f"{cat} Item #{pid}",
            "category": cat,
            "cost_price": cost_price,
            "base_price": base_price,
            "is_active": True,
            "updated_at": datetime.now(timezone.utc).isoformat()
        })
    return products


def generate_users_with_scd2_history(num_users=200):
    """
    Generates users with updates over time to test SCD Type 2 logic.
    """
    scd2_events = []
    start_time = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    
    for uid in range(1001, 1001 + num_users):
        name = f"Customer_{uid}"
        email = f"user_{uid}@example.com"
        initial_region = random.choice(REGIONS)
        initial_tier, initial_discount = LOYALTY_TIERS[0]
        
        # Initial user state
        event_time_1 = start_time + timedelta(days=random.randint(0, 30))
        scd2_events.append({
            "user_id": uid,
            "name": name,
            "email": email,
            "region": initial_region,
            "loyalty_tier": initial_tier,
            "discount_rate": initial_discount,
            "updated_at": event_time_1.isoformat(),
            "batch_id": "batch_initial"
        })
        
        # ~40% of users update their tier or region later
        if random.random() < 0.40:
            new_region = random.choice(REGIONS) if random.random() < 0.3 else initial_region
            new_tier, new_discount = random.choice(LOYALTY_TIERS[1:])
            event_time_2 = event_time_1 + timedelta(days=random.randint(20, 60))
            
            scd2_events.append({
                "user_id": uid,
                "name": name,
                "email": email,
                "region": new_region,
                "loyalty_tier": new_tier,
                "discount_rate": new_discount,
                "updated_at": event_time_2.isoformat(),
                "batch_id": "batch_tier_upgrade"
            })
            
    return scd2_events


def generate_clickstream_and_orders(products, users_events, num_sessions=500):
    """
    Generates browsing sessions and resulting orders:
    view_item -> add_to_cart -> checkout -> purchase
    """
    clickstream_events = []
    orders = []
    order_items = []
    
    order_counter = 50001
    item_counter = 90001
    
    now = datetime.now(timezone.utc)
    start_window = now - timedelta(hours=48)
    user_ids = list({u["user_id"] for u in users_events})
    
    for _ in range(num_sessions):
        user_id = random.choice(user_ids)
        session_id = str(uuid.uuid4())
        device = random.choice(DEVICE_TYPES)
        region = random.choice(REGIONS)
        session_time = start_window + timedelta(minutes=random.randint(0, 48 * 60))
        
        browsed_products = random.sample(products, k=random.randint(1, 4))
        for p in browsed_products:
            # 1. view_item
            view_time = session_time + timedelta(seconds=random.randint(5, 60))
            event_id = str(uuid.uuid4())
            view_event = {
                "event_id": event_id,
                "event_time": view_time.strftime("%Y-%m-%d %H:%M:%S"),
                "event_type": "view_item",
                "user_id": user_id,
                "session_id": session_id,
                "product_id": p["product_id"],
                "category": p["category"],
                "price": float(p["base_price"]),
                "device_type": device,
                "region": region,
                "ip_address": f"192.168.{random.randint(1,254)}.{random.randint(1,254)}",
                "event_version": 1
            }
            clickstream_events.append(view_event)
            
            # Simulate occasional network retry with duplicate event_id
            if random.random() < 0.05:
                dup = view_event.copy()
                dup["event_version"] = 2
                clickstream_events.append(dup)
            
            # 2. add_to_cart (35% conversion from view)
            if random.random() < 0.35:
                cart_time = view_time + timedelta(seconds=random.randint(10, 120))
                clickstream_events.append({
                    "event_id": str(uuid.uuid4()),
                    "event_time": cart_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "event_type": "add_to_cart",
                    "user_id": user_id,
                    "session_id": session_id,
                    "product_id": p["product_id"],
                    "category": p["category"],
                    "price": float(p["base_price"]),
                    "device_type": device,
                    "region": region,
                    "ip_address": view_event["ip_address"],
                    "event_version": 1
                })
                
                # 3. purchase (45% conversion from cart)
                if random.random() < 0.45:
                    purchase_time = cart_time + timedelta(seconds=random.randint(30, 300))
                    clickstream_events.append({
                        "event_id": str(uuid.uuid4()),
                        "event_time": purchase_time.strftime("%Y-%m-%d %H:%M:%S"),
                        "event_type": "purchase",
                        "user_id": user_id,
                        "session_id": session_id,
                        "product_id": p["product_id"],
                        "category": p["category"],
                        "price": float(p["base_price"]),
                        "device_type": device,
                        "region": region,
                        "ip_address": view_event["ip_address"],
                        "event_version": 1
                    })
                    
                    quantity = random.randint(1, 3)
                    discount = round(p["base_price"] * random.choice([0.0, 0.05, 0.10]), 2)
                    unit_p = p["base_price"]
                    tax = round((unit_p * quantity - discount) * 0.10, 2)
                    delivery_cost = round(random.choice([0.0, 5.0, 12.0]), 2)
                    
                    status_rand = random.random()
                    if status_rand < 0.75:
                        status = "DELIVERED"
                    elif status_rand < 0.90:
                        status = "PAID"
                    elif status_rand < 0.95:
                        status = "REFUNDED"
                    else:
                        status = "CANCELLED"
                        
                    orders.append({
                        "order_id": order_counter,
                        "user_id": user_id,
                        "order_timestamp": purchase_time.strftime("%Y-%m-%d %H:%M:%S"),
                        "status": status,
                        "delivery_cost": delivery_cost,
                        "batch_id": "batch_cdc"
                    })
                    
                    order_items.append({
                        "item_id": item_counter,
                        "order_id": order_counter,
                        "product_id": p["product_id"],
                        "quantity": quantity,
                        "unit_price": unit_p,
                        "discount_amount": discount,
                        "tax_amount": tax,
                        "batch_id": "batch_cdc"
                    })
                    
                    order_counter += 1
                    item_counter += 1

    return clickstream_events, orders, order_items


def main():
    parser = argparse.ArgumentParser(description="Synthetic traffic generator for OmniRetail")
    parser.add_argument("--output-dir", default="./data_sample", help="Output directory for generated JSON files")
    parser.add_argument("--products", type=int, default=50, help="Number of products")
    parser.add_argument("--users", type=int, default=200, help="Number of users")
    parser.add_argument("--sessions", type=int, default=800, help="Number of user sessions")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    products = generate_catalog(args.products)
    users_events = generate_users_with_scd2_history(args.users)
    clickstream, orders, order_items = generate_clickstream_and_orders(products, users_events, args.sessions)
    
    datasets = {
        "products_catalog.json": products,
        "users_scd2_raw.json": users_events,
        "clickstream_events.json": clickstream,
        "orders_cdc.json": orders,
        "order_items_cdc.json": order_items,
    }
    
    for filename, data in datasets.items():
        path = os.path.join(args.output_dir, filename)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Saved {path} ({len(data)} records)")
        
    print(f"Generated {len(clickstream)} clickstream events, {len(orders)} orders, and {len(users_events)} user revisions.")


if __name__ == "__main__":
    main()
