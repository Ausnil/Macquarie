import pandas as pd
from datetime import datetime, timedelta
import sqlite3
from typing import Dict, List, Optional

class DataProcessor:
    def __init__(self, db_path: str = 'customer_data.db'):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        """Initialize database with proper schema"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute('''
                CREATE TABLE IF NOT EXISTS customers (
                    customer_id TEXT PRIMARY KEY,
                    name TEXT,
                    email TEXT,
                    dob TEXT,
                    address TEXT,
                    created_date TEXT,
                    last_updated TEXT
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS address_changes (
                    change_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    customer_id TEXT,
                    old_address TEXT,
                    new_address TEXT,
                    change_date TEXT,
                    source_file TEXT,
                    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
                )
            ''')
            conn.commit()

    def parse_customer_string(self, customer_str: str) -> Dict:
        """Parse customer data from {ID_Name_Email...} format"""
        try:
            stripped = customer_str.strip('{}')
            parts = stripped.split('_', 5)
            excel_date = float(parts[5])
            created_date = datetime(1899, 12, 30) + timedelta(days=excel_date)
            
            return {
                'customer_id': parts[0],
                'name': parts[1],
                'email': parts[2],
                'dob': parts[3],
                'address': parts[4],
                'created_date': created_date.isoformat(),
                'excel_date': excel_date
            }
        except Exception as e:
            raise ValueError(f"Error parsing customer string: {str(e)}")

    def _get_address_history(self, customer_id: str) -> pd.DataFrame:
        """Get address history for a specific customer"""
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql('''
                SELECT old_address, new_address, change_date 
                FROM address_changes 
                WHERE customer_id = ?
                ORDER BY change_date DESC
            ''', conn, params=(customer_id,))

    def process_data(self, customers: pd.DataFrame, 
                   transactions: pd.DataFrame, 
                   products: pd.DataFrame) -> Dict:
        """Process data with complete address history"""
        results = {
            'customers_with_history': pd.DataFrame(),
            'customer_category_totals': pd.DataFrame(),
            'top_spenders': pd.DataFrame(),
            'customer_rankings': pd.DataFrame(),
            'summary': [],
            'errors': []
        }

        try:
            # Parse customers
            customers_df = pd.DataFrame([
                self.parse_customer_string(s) for s in customers.iloc[:, 0]
            ])
            
            # Process transactions
            transactions['transaction_date'] = (
                pd.to_datetime(transactions['transaction_date'], unit='D', origin='1899-12-30')
                .apply(lambda x: x.isoformat())
            )

            with sqlite3.connect(self.db_path) as conn:
                # Process each customer and track changes
                for _, customer in customers_df.iterrows():
                    try:
                        existing = conn.execute(
                            'SELECT address FROM customers WHERE customer_id = ?',
                            (customer['customer_id'],)
                        ).fetchone()

                        if existing:
                            if existing[0] != customer['address']:
                                conn.execute('''
                                    INSERT INTO address_changes (
                                        customer_id, old_address, new_address, change_date
                                    ) VALUES (?, ?, ?, datetime('now'))
                                ''', (
                                    customer['customer_id'],
                                    existing[0],
                                    customer['address']
                                ))
                            
                            conn.execute('''
                                UPDATE customers SET
                                    address = ?,
                                    last_updated = datetime('now')
                                WHERE customer_id = ?
                            ''', (
                                customer['address'],
                                customer['customer_id']
                            ))
                        else:
                            conn.execute('''
                                INSERT INTO customers (
                                    customer_id, name, email, dob, address, created_date, last_updated
                                ) VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
                            ''', (
                                customer['customer_id'],
                                customer['name'],
                                customer['email'],
                                customer['dob'],
                                customer['address'],
                                customer['created_date'],
                            ))
                    except Exception as e:
                        results['errors'].append(f"Error processing {customer['customer_id']}: {str(e)}")
                
                conn.commit()
                
                # Get current customers with their address history
                current_customers = pd.read_sql('SELECT * FROM customers', conn)
                current_customers['address_history'] = current_customers['customer_id'].apply(
                    lambda x: self._get_address_history(x).to_dict('records')
                )
                results['customers_with_history'] = current_customers

                # Get all address changes
                address_changes = pd.read_sql('SELECT * FROM address_changes', conn)

            # Merge data for analysis
            merged = pd.merge(
                pd.merge(transactions, products, on='product_code'),
                current_customers,
                on='customer_id'
            )

            # Calculate customer rankings
            customer_rankings = merged.groupby(['customer_id', 'name'])['amount'].sum()
            customer_rankings = customer_rankings.reset_index()
            customer_rankings = customer_rankings.rename(columns={'amount': 'total_spent'})
            customer_rankings = customer_rankings.sort_values('total_spent', ascending=False)
            results['customer_rankings'] = customer_rankings

            # Calculate category totals
            category_totals = merged.groupby(['customer_id', 'category', 'name'])['amount'].sum()
            results['customer_category_totals'] = category_totals.reset_index()

            # Find top spenders per category
            results['top_spenders'] = results['customer_category_totals'].loc[
                results['customer_category_totals'].groupby('category')['amount'].idxmax()
            ]

            # Generate summary
            results['summary'] = [
                f"Total customers: {len(current_customers)}",
                f"Total address changes: {len(address_changes)}",
                f"Top customer: {results['customer_rankings'].iloc[0]['name']} (${results['customer_rankings'].iloc[0]['total_spent']:.2f})"
            ]

        except Exception as e:
            raise ValueError(f"Data processing failed: {str(e)}")

        return results

# Backward compatibility
def process_data(customers, transactions, products):
    return DataProcessor().process_data(customers, transactions, products)
