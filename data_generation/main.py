from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta
import csv


def generate_customers(n_customers=500):
    """Generate customer data for HVAC company."""
    fake = Faker()
    customers = []
    
    for i in range(n_customers):
        customers.append({
            'customer_id': i + 1,
            'customer_name': fake.company() if random.choice([True, False]) else fake.name(),
            'address': fake.address().replace('\n', ', '),
            'phone': fake.phone_number(),
            'customer_type': random.choice(['residential', 'commercial']),
            'created_at': fake.date_between(start_date='-2y', end_date='today')
        })
    
    return pd.DataFrame(customers)


def generate_technicians(n_technicians=15):
    """Generate technician data with detailed skill levels."""
    fake = Faker()
    technicians = []
    
    levels = ['junior', 'senior', 'lead', 'specialist']
    skills = ['HVAC_Installation', 'Electrical', 'Refrigeration', 'Ductwork', 
              'Diagnostics', 'Customer_Service', 'Safety_Protocols']
    
    for i in range(n_technicians):
        level = random.choice(levels)
        # Skill levels based on technician level
        base_skill = {'junior': 3, 'senior': 6, 'lead': 8, 'specialist': 9}[level]
        
        technician_skills = {}
        for skill in skills:
            skill_variance = random.randint(-2, 2)
            skill_level = max(1, min(10, base_skill + skill_variance))
            technician_skills[f'{skill.lower()}_skill'] = skill_level
        
        technician = {
            'technician_id': i + 1,
            'technician_name': fake.name(),
            'phone': fake.phone_number(),
            'technician_level': level,
            'hourly_rate': random.randint(25, 75),
            'hire_date': fake.date_between(start_date='-5y', end_date='today'),
            'years_experience': random.randint(1, 20),
            'certification_level': random.choice(['Basic', 'Intermediate', 'Advanced', 'Master'])
        }
        technician.update(technician_skills)
        technicians.append(technician)
    
    return pd.DataFrame(technicians)


def generate_equipment_types(n_equipment=50):
    """Generate equipment type data."""
    fake = Faker()
    equipment = []
    
    brands = ['Carrier', 'Trane', 'Lennox', 'Rheem', 'Goodman', 'York', 'Daikin']
    types = ['Central AC', 'Heat Pump', 'Furnace', 'Ductless Mini-Split', 'Package Unit']
    
    for i in range(n_equipment):
        equipment.append({
            'equipment_id': i + 1,
            'brand': random.choice(brands),
            'model': f"{random.choice(['X', 'Pro', 'Elite', 'Prime'])}{random.randint(100, 999)}",
            'equipment_type': random.choice(types),
            'btu_rating': random.choice([18000, 24000, 36000, 48000, 60000]),
            'energy_rating': round(random.uniform(13, 20), 1)
        })
    
    return pd.DataFrame(equipment)


def generate_parts(n_parts=200):
    """Generate parts inventory data."""
    fake = Faker()
    parts = []
    
    part_types = ['Filter', 'Compressor', 'Coil', 'Fan Motor', 'Thermostat', 
                  'Capacitor', 'Contactor', 'Refrigerant', 'Ductwork', 'Valve']
    
    for i in range(n_parts):
        parts.append({
            'part_id': i + 1,
            'part_name': f"{random.choice(part_types)} - {fake.word().title()}",
            'part_category': random.choice(part_types),
            'unit_cost': round(random.uniform(5, 500), 2),
            'supplier': fake.company()
        })
    
    return pd.DataFrame(parts)


def generate_installed_devices(customers_df, equipment_df, n_installations=800):
    """Generate installed devices data tracking customer equipment."""
    fake = Faker()
    installations = []
    
    statuses = ['Active', 'Inactive', 'Under Maintenance', 'Replaced']
    
    for i in range(n_installations):
        install_date = fake.date_between(start_date='-3y', end_date='today')
        warranty_months = random.choice([12, 24, 36, 60])
        warranty_end = install_date + timedelta(days=warranty_months * 30)
        
        installations.append({
            'installation_id': i + 1,
            'customer_id': random.choice(customers_df['customer_id'].tolist()),
            'equipment_id': random.choice(equipment_df['equipment_id'].tolist()),
            'installation_date': install_date,
            'warranty_end_date': warranty_end,
            'serial_number': fake.bothify('??##-####-####'),
            'installation_location': random.choice(['Main Floor', 'Basement', 'Attic', 'Garage', 'Roof']),
            'status': random.choice(statuses),
            'last_maintenance_date': fake.date_between(start_date=install_date, end_date=max(install_date, datetime.now().date())) if random.choice([True, False]) else None
        })
    
    return pd.DataFrame(installations)


def generate_service_calls(customers_df, technicians_df, equipment_df, n_calls=2000):
    """Generate service call data."""
    fake = Faker()
    service_calls = []
    
    service_types = ['Maintenance', 'Repair', 'Installation', 'Emergency', 'Inspection']
    
    for i in range(n_calls):
        service_date = fake.date_between(start_date='-1y', end_date='today')
        duration = round(random.uniform(1, 8), 1)
        
        service_calls.append({
            'service_call_id': i + 1,
            'customer_id': random.choice(customers_df['customer_id'].tolist()),
            'technician_id': random.choice(technicians_df['technician_id'].tolist()),
            'equipment_id': random.choice(equipment_df['equipment_id'].tolist()),
            'service_date': service_date,
            'service_type': random.choice(service_types),
            'duration_hours': duration,
            'labor_cost': round(duration * random.randint(50, 100), 2),
            'parts_cost': round(random.uniform(0, 300), 2),
            'total_cost': 0  # Will calculate after
        })
    
    df = pd.DataFrame(service_calls)
    df['total_cost'] = df['labor_cost'] + df['parts_cost']
    return df


def generate_parts_usage(service_calls_df, parts_df, avg_parts_per_call=2):
    """Generate parts usage data."""
    parts_usage = []
    
    for _, call in service_calls_df.iterrows():
        n_parts = random.randint(0, avg_parts_per_call * 2)
        
        for _ in range(n_parts):
            parts_usage.append({
                'usage_id': len(parts_usage) + 1,
                'service_call_id': call['service_call_id'],
                'part_id': random.choice(parts_df['part_id'].tolist()),
                'quantity_used': random.randint(1, 5),
                'usage_date': call['service_date']
            })
    
    return pd.DataFrame(parts_usage)


def generate_incident_response(service_calls_df, technicians_df, n_incidents=150):
    """Generate incident response data for emergency calls."""
    fake = Faker()
    incidents = []
    
    # Filter for emergency service calls
    emergency_calls = service_calls_df[service_calls_df['service_type'] == 'Emergency']
    
    incident_types = ['No Heat/AC', 'Gas Leak', 'Electrical Issue', 'Water Damage', 
                     'System Failure', 'Carbon Monoxide Alert']
    severity_levels = ['Low', 'Medium', 'High', 'Critical']
    resolution_status = ['Resolved', 'Pending', 'Escalated', 'Requires Follow-up']
    
    for i in range(min(n_incidents, len(emergency_calls))):
        call = emergency_calls.iloc[i % len(emergency_calls)]
        
        response_time = random.randint(15, 240)  # minutes
        resolution_time = random.randint(30, 480)  # minutes
        
        incidents.append({
            'incident_id': i + 1,
            'service_call_id': call['service_call_id'],
            'incident_type': random.choice(incident_types),
            'severity_level': random.choice(severity_levels),
            'reported_time': fake.date_time_between(start_date=call['service_date'], end_date=call['service_date']),
            'response_time_minutes': response_time,
            'resolution_time_minutes': resolution_time,
            'responding_technician_id': call['technician_id'],
            'backup_technician_id': random.choice(technicians_df['technician_id'].tolist()) if random.choice([True, False]) else None,
            'resolution_status': random.choice(resolution_status),
            'customer_satisfaction': random.randint(1, 10),
            'follow_up_required': random.choice([True, False])
        })
    
    return pd.DataFrame(incidents)


def generate_vehicle_fleet(technicians_df, n_vehicles=20):
    """Generate vehicle fleet data for company vehicles."""
    fake = Faker()
    vehicles = []
    
    vehicle_types = ['Van', 'Truck', 'SUV']
    makes = ['Ford', 'Chevrolet', 'Ram', 'GMC', 'Mercedes']
    statuses = ['Active', 'Maintenance', 'Out of Service', 'Retired']
    
    for i in range(n_vehicles):
        purchase_date = fake.date_between(start_date='-8y', end_date='-1y')
        mileage = random.randint(20000, 150000)
        
        vehicles.append({
            'vehicle_id': i + 1,
            'vehicle_number': f"HVAC-{i+1:03d}",
            'make': random.choice(makes),
            'model': fake.word().title(),
            'year': random.randint(2015, 2023),
            'vehicle_type': random.choice(vehicle_types),
            'license_plate': fake.license_plate(),
            'vin': fake.bothify('?#?##?##?#?######'),
            'assigned_technician_id': random.choice(technicians_df['technician_id'].tolist()) if random.choice([True, False]) else None,
            'purchase_date': purchase_date,
            'current_mileage': mileage,
            'last_maintenance_date': fake.date_between(start_date=purchase_date, end_date=max(purchase_date, datetime.now().date())),
            'next_maintenance_mileage': mileage + random.randint(3000, 8000),
            'status': random.choice(statuses),
            'fuel_type': random.choice(['Gasoline', 'Diesel', 'Hybrid']),
            'gps_enabled': random.choice([True, False])
        })
    
    return pd.DataFrame(vehicles)


def generate_mailing_list(customers_df, n_additional=300):
    """Generate mailing list data for marketing contacts."""
    fake = Faker()
    mailing_list = []
    
    # Include existing customers
    for _, customer in customers_df.iterrows():
        mailing_list.append({
            'contact_id': len(mailing_list) + 1,
            'customer_id': customer['customer_id'],
            'email': fake.email(),
            'first_name': customer['customer_name'].split()[0] if ' ' in customer['customer_name'] else customer['customer_name'],
            'last_name': customer['customer_name'].split()[-1] if ' ' in customer['customer_name'] else '',
            'phone': customer['phone'],
            'address': customer['address'],
            'contact_source': 'Existing Customer',
            'subscription_status': random.choice(['Subscribed', 'Unsubscribed']),
            'subscription_date': fake.date_between(start_date='-2y', end_date='today'),
            'preferred_contact_method': random.choice(['Email', 'Phone', 'Mail']),
            'interests': random.choice(['Maintenance Tips', 'Seasonal Offers', 'New Products', 'Energy Efficiency'])
        })
    
    # Add additional prospects
    for i in range(n_additional):
        mailing_list.append({
            'contact_id': len(mailing_list) + 1,
            'customer_id': None,
            'email': fake.email(),
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'phone': fake.phone_number(),
            'address': fake.address().replace('\n', ', '),
            'contact_source': random.choice(['Website', 'Referral', 'Trade Show', 'Social Media', 'Direct Mail']),
            'subscription_status': random.choice(['Subscribed', 'Unsubscribed']),
            'subscription_date': fake.date_between(start_date='-1y', end_date='today'),
            'preferred_contact_method': random.choice(['Email', 'Phone', 'Mail']),
            'interests': random.choice(['Maintenance Tips', 'Seasonal Offers', 'New Products', 'Energy Efficiency'])
        })
    
    return pd.DataFrame(mailing_list)


def generate_subscriptions(customers_df, n_subscriptions=250):
    """Generate subscription/service contract data."""
    fake = Faker()
    subscriptions = []
    
    service_plans = ['Basic', 'Premium', 'Comprehensive', 'Commercial']
    plan_costs = {'Basic': 120, 'Premium': 200, 'Comprehensive': 350, 'Commercial': 500}
    statuses = ['Active', 'Expired', 'Cancelled', 'Suspended']
    
    for i in range(n_subscriptions):
        start_date = fake.date_between(start_date='-2y', end_date='today')
        plan = random.choice(service_plans)
        
        # Contract duration typically 1-3 years
        contract_months = random.choice([12, 24, 36])
        end_date = start_date + timedelta(days=contract_months * 30)
        
        subscriptions.append({
            'subscription_id': i + 1,
            'customer_id': random.choice(customers_df['customer_id'].tolist()),
            'service_plan': plan,
            'start_date': start_date,
            'end_date': end_date,
            'annual_cost': plan_costs[plan],
            'payment_frequency': random.choice(['Monthly', 'Quarterly', 'Annual']),
            'status': random.choice(statuses),
            'auto_renewal': random.choice([True, False]),
            'services_included': random.choice([
                'Seasonal Tune-ups, Priority Service',
                'Bi-annual Maintenance, Parts Discount',
                'Quarterly Inspections, Emergency Service',
                'Monthly Monitoring, Full Coverage'
            ]),
            'discount_percentage': random.choice([0, 5, 10, 15, 20]),
            'next_service_date': fake.date_between(start_date='today', end_date='+6m') if random.choice([True, False]) else None
        })
    
    return pd.DataFrame(subscriptions)


def generate_invoices(service_calls_df, customers_df, n_invoices=None):
    """Generate invoice data for completed service calls."""
    fake = Faker()
    invoices = []
    
    # Create invoices for most service calls (some may be warranty/free)
    if n_invoices is None:
        n_invoices = int(len(service_calls_df) * 0.85)
    
    selected_calls = service_calls_df.sample(n=min(n_invoices, len(service_calls_df)))
    
    payment_terms = ['Net 15', 'Net 30', 'Due on Receipt', 'Net 45']
    invoice_status = ['Paid', 'Pending', 'Overdue', 'Cancelled']
    
    for i, (_, call) in enumerate(selected_calls.iterrows()):
        issue_date = call['service_date'] + timedelta(days=random.randint(0, 3))
        due_date = issue_date + timedelta(days=random.choice([15, 30, 45]))
        
        # Add some markup to service call total
        markup = random.uniform(1.1, 1.3)
        subtotal = round(call['total_cost'] * markup, 2)
        tax_rate = random.choice([0.065, 0.075, 0.08, 0.085])  # Various state tax rates
        tax_amount = round(subtotal * tax_rate, 2)
        total_amount = subtotal + tax_amount
        
        invoices.append({
            'invoice_id': i + 1,
            'invoice_number': f"INV-{issue_date.year}-{(i+1):04d}",
            'service_call_id': call['service_call_id'],
            'customer_id': call['customer_id'],
            'issue_date': issue_date,
            'due_date': due_date,
            'payment_terms': random.choice(payment_terms),
            'subtotal': subtotal,
            'tax_rate': tax_rate,
            'tax_amount': tax_amount,
            'total_amount': total_amount,
            'status': random.choice(invoice_status),
            'notes': fake.sentence() if random.choice([True, False]) else None
        })
    
    return pd.DataFrame(invoices)


def generate_payments(invoices_df):
    """Generate payment records for invoices."""
    fake = Faker()
    payments = []
    
    payment_methods = ['Credit Card', 'Check', 'Cash', 'ACH Transfer', 'Online Payment']
    payment_status = ['Completed', 'Pending', 'Failed', 'Refunded']
    
    # Generate payments for paid and some pending invoices
    payable_invoices = invoices_df[invoices_df['status'].isin(['Paid', 'Pending'])]
    
    payment_id = 1
    for _, invoice in payable_invoices.iterrows():
        # Some invoices might have partial payments
        if invoice['status'] == 'Paid':
            # Full payment
            payment_date = fake.date_between(start_date=invoice['issue_date'], end_date=invoice['due_date'] + timedelta(days=30))
            payments.append({
                'payment_id': payment_id,
                'invoice_id': invoice['invoice_id'],
                'payment_date': payment_date,
                'amount': invoice['total_amount'],
                'payment_method': random.choice(payment_methods),
                'transaction_id': fake.bothify('TXN-########'),
                'status': 'Completed',
                'processing_fee': round(invoice['total_amount'] * random.uniform(0.02, 0.035), 2) if random.choice([True, False]) else 0.0,
                'notes': None
            })
            payment_id += 1
            
        elif invoice['status'] == 'Pending' and random.choice([True, False]):
            # Partial payment
            partial_amount = round(invoice['total_amount'] * random.uniform(0.3, 0.8), 2)
            payment_date = fake.date_between(start_date=invoice['issue_date'], end_date=max(invoice['issue_date'], datetime.now().date()))
            payments.append({
                'payment_id': payment_id,
                'invoice_id': invoice['invoice_id'],
                'payment_date': payment_date,
                'amount': partial_amount,
                'payment_method': random.choice(payment_methods),
                'transaction_id': fake.bothify('TXN-########'),
                'status': random.choice(['Completed', 'Pending']),
                'processing_fee': round(partial_amount * random.uniform(0.02, 0.035), 2) if random.choice([True, False]) else 0.0,
                'notes': 'Partial payment'
            })
            payment_id += 1
    
    return pd.DataFrame(payments)


def generate_inventory(parts_df):
    """Generate inventory management data for parts stock levels."""
    fake = Faker()
    inventory = []
    
    for _, part in parts_df.iterrows():
        current_stock = random.randint(0, 100)
        reorder_point = random.randint(5, 25)
        max_stock = random.randint(50, 200)
        
        # Calculate some basic inventory metrics
        last_restock_date = fake.date_between(start_date='-6m', end_date='today')
        avg_monthly_usage = random.randint(2, 30)
        
        inventory.append({
            'inventory_id': part['part_id'],
            'part_id': part['part_id'],
            'warehouse_location': f"{random.choice(['A', 'B', 'C'])}-{random.randint(1, 20):02d}-{random.randint(1, 10)}",
            'current_stock': current_stock,
            'reorder_point': reorder_point,
            'max_stock_level': max_stock,
            'last_restock_date': last_restock_date,
            'last_restock_quantity': random.randint(20, 100),
            'avg_monthly_usage': avg_monthly_usage,
            'stock_status': 'Low Stock' if current_stock <= reorder_point else 'In Stock',
            'supplier_lead_time_days': random.randint(3, 21),
            'abc_classification': random.choice(['A', 'B', 'C']),  # ABC analysis classification
            'last_count_date': fake.date_between(start_date='-3m', end_date='today'),
            'unit_cost': part['unit_cost'],
            'total_value': round(current_stock * part['unit_cost'], 2)
        })
    
    return pd.DataFrame(inventory)


def generate_appointments(customers_df, technicians_df, service_calls_df, n_appointments=500):
    """Generate appointment scheduling data including future appointments."""
    fake = Faker()
    appointments = []
    
    appointment_types = ['Maintenance', 'Repair', 'Installation', 'Inspection', 'Emergency', 'Consultation']
    appointment_status = ['Scheduled', 'Confirmed', 'Completed', 'Cancelled', 'No-Show', 'Rescheduled']
    
    appointment_id = 1
    
    # Create appointments for existing service calls (historical)
    for _, call in service_calls_df.iterrows():
        scheduled_time = fake.date_time_between(
            start_date=call['service_date'] - timedelta(days=14),
            end_date=call['service_date']
        )
        
        appointments.append({
            'appointment_id': appointment_id,
            'customer_id': call['customer_id'],
            'technician_id': call['technician_id'],
            'service_call_id': call['service_call_id'],
            'appointment_date': call['service_date'],
            'scheduled_time': scheduled_time,
            'appointment_type': call['service_type'],
            'estimated_duration_hours': call['duration_hours'],
            'status': 'Completed',
            'priority': random.choice(['Low', 'Medium', 'High', 'Emergency']),
            'special_instructions': fake.sentence() if random.choice([True, False]) else None,
            'created_date': scheduled_time - timedelta(days=random.randint(1, 7)),
            'confirmed_date': scheduled_time + timedelta(hours=random.randint(1, 48)) if random.choice([True, False]) else None
        })
        appointment_id += 1
    
    # Create future appointments
    remaining_appointments = n_appointments - len(appointments)
    for i in range(remaining_appointments):
        future_date = fake.date_between(start_date='today', end_date='+3m')
        scheduled_time = fake.date_time_between_dates(
            datetime_start=datetime.combine(future_date, datetime.min.time()),
            datetime_end=datetime.combine(future_date, datetime.max.time())
        )
        
        appointments.append({
            'appointment_id': appointment_id,
            'customer_id': random.choice(customers_df['customer_id'].tolist()),
            'technician_id': random.choice(technicians_df['technician_id'].tolist()),
            'service_call_id': None,  # Future appointments don't have service calls yet
            'appointment_date': future_date,
            'scheduled_time': scheduled_time,
            'appointment_type': random.choice(appointment_types),
            'estimated_duration_hours': round(random.uniform(1, 6), 1),
            'status': random.choice(['Scheduled', 'Confirmed']),
            'priority': random.choice(['Low', 'Medium', 'High']),
            'special_instructions': fake.sentence() if random.choice([True, False]) else None,
            'created_date': fake.date_between(start_date='-2w', end_date='today'),
            'confirmed_date': fake.date_between(start_date='-1w', end_date='today') if random.choice([True, False]) else None
        })
        appointment_id += 1
    
    return pd.DataFrame(appointments)


def generate_quotes(customers_df, equipment_df, n_quotes=400):
    """Generate sales quotes/estimates for potential work."""
    fake = Faker()
    quotes = []
    
    quote_types = ['Repair', 'Installation', 'Replacement', 'Maintenance Contract', 'System Upgrade']
    quote_status = ['Pending', 'Accepted', 'Declined', 'Expired', 'Revised']
    
    for i in range(n_quotes):
        quote_date = fake.date_between(start_date='-6m', end_date='today')
        valid_until = quote_date + timedelta(days=random.randint(15, 45))
        
        # Generate quote amounts
        labor_hours = round(random.uniform(2, 16), 1)
        labor_rate = random.randint(75, 125)
        labor_cost = labor_hours * labor_rate
        
        equipment_cost = random.uniform(500, 8000) if random.choice([True, False]) else 0
        parts_cost = random.uniform(50, 1200)
        subtotal = labor_cost + equipment_cost + parts_cost
        markup = random.uniform(1.1, 1.4)
        total_amount = round(subtotal * markup, 2)
        
        quotes.append({
            'quote_id': i + 1,
            'quote_number': f"QUO-{quote_date.year}-{(i+1):04d}",
            'customer_id': random.choice(customers_df['customer_id'].tolist()),
            'equipment_id': random.choice(equipment_df['equipment_id'].tolist()) if equipment_cost > 0 else None,
            'quote_date': quote_date,
            'valid_until': valid_until,
            'quote_type': random.choice(quote_types),
            'description': fake.text(max_nb_chars=200),
            'labor_hours': labor_hours,
            'labor_rate': labor_rate,
            'labor_cost': labor_cost,
            'equipment_cost': round(equipment_cost, 2),
            'parts_cost': round(parts_cost, 2),
            'total_amount': total_amount,
            'status': random.choice(quote_status),
            'created_by': f"Sales Rep {random.randint(1, 5)}",
            'follow_up_date': quote_date + timedelta(days=random.randint(3, 14)) if random.choice([True, False]) else None,
            'notes': fake.sentence() if random.choice([True, False]) else None
        })
    
    return pd.DataFrame(quotes)


def generate_work_orders(service_calls_df, technicians_df):
    """Generate detailed work orders for service calls."""
    fake = Faker()
    work_orders = []
    
    work_order_status = ['Open', 'In Progress', 'Completed', 'On Hold', 'Cancelled']
    
    for _, call in service_calls_df.iterrows():
        work_orders.append({
            'work_order_id': call['service_call_id'],
            'service_call_id': call['service_call_id'],
            'technician_id': call['technician_id'],
            'work_order_number': f"WO-{call['service_date'].year}-{call['service_call_id']:04d}",
            'description': fake.text(max_nb_chars=300),
            'instructions': fake.text(max_nb_chars=200),
            'safety_requirements': random.choice(['Standard PPE', 'Electrical Safety', 'Confined Space', 'Height Work']),
            'estimated_hours': call['duration_hours'],
            'actual_hours': call['duration_hours'] + random.uniform(-0.5, 1.0),
            'status': 'Completed',
            'priority': random.choice(['Low', 'Medium', 'High', 'Emergency']),
            'completion_notes': fake.text(max_nb_chars=150),
            'customer_signature_required': random.choice([True, False])
        })
    
    return pd.DataFrame(work_orders)


def generate_customer_feedback(service_calls_df, customers_df, n_feedback=800):
    """Generate customer feedback and satisfaction surveys."""
    fake = Faker()
    feedback = []
    
    feedback_types = ['Survey', 'Review', 'Complaint', 'Compliment', 'Suggestion']
    
    # Sample service calls for feedback
    selected_calls = service_calls_df.sample(n=min(n_feedback, len(service_calls_df)))
    
    for i, (_, call) in enumerate(selected_calls.iterrows()):
        feedback_date = call['service_date'] + timedelta(days=random.randint(1, 14))
        
        feedback.append({
            'feedback_id': i + 1,
            'service_call_id': call['service_call_id'],
            'customer_id': call['customer_id'],
            'feedback_date': feedback_date,
            'feedback_type': random.choice(feedback_types),
            'overall_satisfaction': random.randint(1, 10),
            'technician_rating': random.randint(1, 10),
            'timeliness_rating': random.randint(1, 10),
            'quality_rating': random.randint(1, 10),
            'value_rating': random.randint(1, 10),
            'comments': fake.text(max_nb_chars=250),
            'would_recommend': random.choice([True, False]),
            'follow_up_required': random.choice([True, False]),
            'source': random.choice(['Email Survey', 'Phone Call', 'Online Review', 'In Person'])
        })
    
    return pd.DataFrame(feedback)


def generate_leads(n_leads=600):
    """Generate sales leads for potential customers."""
    fake = Faker()
    leads = []
    
    lead_sources = ['Website', 'Referral', 'Cold Call', 'Trade Show', 'Social Media', 'Advertisement']
    lead_status = ['New', 'Contacted', 'Qualified', 'Converted', 'Lost', 'Nurturing']
    service_interest = ['AC Repair', 'Heating Repair', 'Installation', 'Maintenance', 'Emergency Service']
    
    for i in range(n_leads):
        created_date = fake.date_between(start_date='-1y', end_date='today')
        
        leads.append({
            'lead_id': i + 1,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'company_name': fake.company() if random.choice([True, False]) else None,
            'phone': fake.phone_number(),
            'email': fake.email(),
            'address': fake.address().replace('\n', ', '),
            'lead_source': random.choice(lead_sources),
            'service_interest': random.choice(service_interest),
            'estimated_value': round(random.uniform(200, 5000), 2),
            'urgency': random.choice(['Low', 'Medium', 'High']),
            'status': random.choice(lead_status),
            'created_date': created_date,
            'last_contact_date': fake.date_between(start_date=created_date, end_date=max(created_date, datetime.now().date())) if random.choice([True, False]) else None,
            'assigned_to': f"Sales Rep {random.randint(1, 5)}",
            'notes': fake.text(max_nb_chars=200),
            'conversion_probability': random.randint(10, 90)
        })
    
    return pd.DataFrame(leads)


def save_to_csv(dataframes, output_dir='../hvac_analytics/seeds/', incremental=False):
    """Save all dataframes to CSV files with incremental support."""
    import os
    
    print(f"DEBUG: incremental={incremental}")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    for name, df in dataframes.items():
        filename = f"{output_dir}{name}.csv"
        file_exists = os.path.exists(filename)
        
        print(f"DEBUG: {name} - incremental={incremental}, file_exists={file_exists}")
        
        if incremental and file_exists:
            # Append to existing file
            df.to_csv(filename, mode='a', header=False, index=False)
            print(f"Appended {len(df)} records to {filename}")
        else:
            # Create new file or overwrite
            df.to_csv(filename, index=False)
            action = "Overwritten" if file_exists else "Created"
            print(f"{action} {filename} with {len(df)} records")


def generate_incremental_data():
    """Generate realistic incremental data for existing datasets."""
    print("Generating incremental HVAC data...")
    
    # Read existing data to get current IDs
    import os
    import pandas as pd
    
    seeds_dir = '../hvac_analytics/seeds/'
    
    # Load existing data to determine next IDs
    try:
        existing_customers = pd.read_csv(f'{seeds_dir}customers.csv')
        existing_technicians = pd.read_csv(f'{seeds_dir}technicians.csv') 
        existing_equipment = pd.read_csv(f'{seeds_dir}equipment_types.csv')
        existing_parts = pd.read_csv(f'{seeds_dir}parts.csv')
        existing_service_calls = pd.read_csv(f'{seeds_dir}service_calls.csv')
        
        next_customer_id = existing_customers['customer_id'].max() + 1
        next_technician_id = existing_technicians['technician_id'].max() + 1
        next_equipment_id = existing_equipment['equipment_id'].max() + 1
        next_part_id = existing_parts['part_id'].max() + 1
        next_service_call_id = existing_service_calls['service_call_id'].max() + 1
        
    except FileNotFoundError:
        print("No existing data found. Run full generation first.")
        return
    
    # Generate small increments (realistic business growth)
    new_customers = generate_customers_incremental(5, next_customer_id)  # 5 new customers
    new_technicians = generate_technicians_incremental(2, next_technician_id)  # 2 new technicians  
    new_equipment = generate_equipment_types_incremental(3, next_equipment_id)  # 3 new equipment types
    new_parts = generate_parts_incremental(10, next_part_id)  # 10 new parts
    
    # More service calls (main business activity)
    all_customers = pd.concat([existing_customers, new_customers])
    all_technicians = pd.concat([existing_technicians, new_technicians])
    all_equipment = pd.concat([existing_equipment, new_equipment])
    
    new_service_calls = generate_service_calls_incremental(200, next_service_call_id, 
                                                          all_customers, all_technicians, all_equipment)
    
    # Generate related data
    new_parts_usage = generate_parts_usage(new_service_calls, pd.concat([existing_parts, new_parts]))
    new_invoices = generate_invoices(new_service_calls, all_customers)
    new_payments = generate_payments(new_invoices)
    new_appointments = generate_appointments(all_customers, all_technicians, new_service_calls, 50)
    new_quotes = generate_quotes(all_customers, all_equipment, 30)
    new_work_orders = generate_work_orders(new_service_calls, all_technicians)
    new_customer_feedback = generate_customer_feedback(new_service_calls, all_customers, 100)
    new_leads = generate_leads(50)
    
    # Package incremental data
    incremental_datasets = {
        'customers': new_customers,
        'technicians': new_technicians,
        'equipment_types': new_equipment,
        'parts': new_parts,
        'service_calls': new_service_calls,
        'parts_usage': new_parts_usage,
        'invoices': new_invoices,
        'payments': new_payments,
        'appointments': new_appointments,
        'quotes': new_quotes,
        'work_orders': new_work_orders,
        'customer_feedback': new_customer_feedback,
        'leads': new_leads
    }
    
    save_to_csv(incremental_datasets, incremental=True)
    
    print(f"\nGenerated incremental HVAC data:")
    total_records = 0
    for name, df in incremental_datasets.items():
        record_count = len(df)
        total_records += record_count
        print(f"- {name}: +{record_count} records")
    
    print(f"\nTotal new records: {total_records}")


def generate_customers_incremental(n_customers, start_id):
    """Generate incremental customer data."""
    fake = Faker()
    customers = []
    
    for i in range(n_customers):
        customers.append({
            'customer_id': start_id + i,
            'customer_name': fake.company() if random.choice([True, False]) else fake.name(),
            'address': fake.address().replace('\n', ', '),
            'phone': fake.phone_number(),
            'customer_type': random.choice(['residential', 'commercial']),
            'created_at': fake.date_between(start_date='-30d', end_date='today')
        })
    
    return pd.DataFrame(customers)


def generate_technicians_incremental(n_technicians, start_id):
    """Generate incremental technician data."""
    fake = Faker()
    technicians = []
    
    levels = ['junior', 'senior', 'lead', 'specialist']
    skills = ['HVAC_Installation', 'Electrical', 'Refrigeration', 'Ductwork', 
              'Diagnostics', 'Customer_Service', 'Safety_Protocols']
    
    for i in range(n_technicians):
        level = random.choice(levels)
        base_skill = {'junior': 3, 'senior': 6, 'lead': 8, 'specialist': 9}[level]
        
        technician_skills = {}
        for skill in skills:
            skill_variance = random.randint(-2, 2)
            skill_level = max(1, min(10, base_skill + skill_variance))
            technician_skills[f'{skill.lower()}_skill'] = skill_level
        
        technician = {
            'technician_id': start_id + i,
            'technician_name': fake.name(),
            'phone': fake.phone_number(),
            'technician_level': level,
            'hourly_rate': random.randint(25, 75),
            'hire_date': fake.date_between(start_date='-90d', end_date='today'),
            'years_experience': random.randint(1, 20),
            'certification_level': random.choice(['Basic', 'Intermediate', 'Advanced', 'Master'])
        }
        technician.update(technician_skills)
        technicians.append(technician)
    
    return pd.DataFrame(technicians)


def generate_equipment_types_incremental(n_equipment, start_id):
    """Generate incremental equipment type data."""
    equipment = []
    
    brands = ['Carrier', 'Trane', 'Lennox', 'Rheem', 'Goodman', 'York', 'Daikin']
    types = ['Central AC', 'Heat Pump', 'Furnace', 'Ductless Mini-Split', 'Package Unit']
    
    for i in range(n_equipment):
        equipment.append({
            'equipment_id': start_id + i,
            'brand': random.choice(brands),
            'model': f"{random.choice(['X', 'Pro', 'Elite', 'Prime'])}{random.randint(100, 999)}",
            'equipment_type': random.choice(types),
            'btu_rating': random.choice([18000, 24000, 36000, 48000, 60000]),
            'energy_rating': round(random.uniform(13, 20), 1)
        })
    
    return pd.DataFrame(equipment)


def generate_parts_incremental(n_parts, start_id):
    """Generate incremental parts data."""
    fake = Faker()
    parts = []
    
    part_types = ['Filter', 'Compressor', 'Coil', 'Fan Motor', 'Thermostat', 
                  'Capacitor', 'Contactor', 'Refrigerant', 'Ductwork', 'Valve']
    
    for i in range(n_parts):
        parts.append({
            'part_id': start_id + i,
            'part_name': f"{random.choice(part_types)} - {fake.word().title()}",
            'part_category': random.choice(part_types),
            'unit_cost': round(random.uniform(5, 500), 2),
            'supplier': fake.company()
        })
    
    return pd.DataFrame(parts)


def generate_service_calls_incremental(n_calls, start_id, customers_df, technicians_df, equipment_df):
    """Generate incremental service call data."""
    fake = Faker()
    service_calls = []
    
    service_types = ['Maintenance', 'Repair', 'Installation', 'Emergency', 'Inspection']
    
    for i in range(n_calls):
        service_date = fake.date_between(start_date='-30d', end_date='today')
        duration = round(random.uniform(1, 8), 1)
        
        service_calls.append({
            'service_call_id': start_id + i,
            'customer_id': random.choice(customers_df['customer_id'].tolist()),
            'technician_id': random.choice(technicians_df['technician_id'].tolist()),
            'equipment_id': random.choice(equipment_df['equipment_id'].tolist()),
            'service_date': service_date,
            'service_type': random.choice(service_types),
            'duration_hours': duration,
            'labor_cost': round(duration * random.randint(50, 100), 2),
            'parts_cost': round(random.uniform(0, 300), 2),
            'total_cost': 0
        })
    
    df = pd.DataFrame(service_calls)
    df['total_cost'] = df['labor_cost'] + df['parts_cost']
    return df


def main():
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == '--incremental':
        generate_incremental_data()
    else:
        print("Generating comprehensive HVAC company training data...")
        
        # Generate all datasets according to specifications
        customers = generate_customers(500)
        technicians = generate_technicians(15)
        equipment = generate_equipment_types(50)
        parts = generate_parts(200)
        installed_devices = generate_installed_devices(customers, equipment, 800)
        service_calls = generate_service_calls(customers, technicians, equipment, 2000)
        parts_usage = generate_parts_usage(service_calls, parts)
        incident_response = generate_incident_response(service_calls, technicians, 150)
        vehicle_fleet = generate_vehicle_fleet(technicians, 20)
        mailing_list = generate_mailing_list(customers, 300)
        subscriptions = generate_subscriptions(customers, 250)
        
        # Generate business operations data
        invoices = generate_invoices(service_calls, customers)
        payments = generate_payments(invoices)
        inventory = generate_inventory(parts)
        appointments = generate_appointments(customers, technicians, service_calls, 500)
        quotes = generate_quotes(customers, equipment, 400)
        work_orders = generate_work_orders(service_calls, technicians)
        customer_feedback = generate_customer_feedback(service_calls, customers, 800)
        leads = generate_leads(600)
        
        # Save to CSV files
        datasets = {
            'customers': customers,
            'technicians': technicians,
            'equipment_types': equipment,
            'parts': parts,
            'installed_devices': installed_devices,
            'service_calls': service_calls,
            'parts_usage': parts_usage,
            'incident_response': incident_response,
            'vehicle_fleet': vehicle_fleet,
            'mailing_list': mailing_list,
            'subscriptions': subscriptions,
            'invoices': invoices,
            'payments': payments,
            'inventory': inventory,
            'appointments': appointments,
            'quotes': quotes,
            'work_orders': work_orders,
            'customer_feedback': customer_feedback,
            'leads': leads
        }
        
        save_to_csv(datasets)
        
        print(f"\nGenerated comprehensive HVAC training data:")
        total_records = 0
        for name, df in datasets.items():
            record_count = len(df)
            total_records += record_count
            print(f"- {name}: {record_count} records")
        
        print(f"\nTotal: {total_records} records across {len(datasets)} tables")


if __name__ == "__main__":
    main()
