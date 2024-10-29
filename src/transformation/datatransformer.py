import redis
import json
import re
import phonenumbers
import logging

class DataTransformer:
    def __init__(self, redis_host="localhost", redis_port=6379, redis_db=0):
        # Initialize Redis connection and logger
        self.redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db)
        self.logger = logging.getLogger(__name__)

    def standardize_phone(self, phone_number):
        """Validates and formats phone numbers using phonenumbers library."""
        try:
            phone_obj = phonenumbers.parse(phone_number)
            if phonenumbers.is_valid_number(phone_obj):
                return phonenumbers.format_number(phone_obj, phonenumbers.PhoneNumberFormat.INTERNATIONAL)
        except phonenumbers.NumberParseException:
            pass
        return None

    def separate_salary_currency(self, salary):
        """Separates salary and currency values."""
        if salary:
            match = re.match(r'([\d,\.]+)([^\d,]+)', salary)
            if match:
                amount = match.group(1).replace(',', '')
                currency = match.group(2).strip()
                return float(amount), currency
        return None, None

    def validate_email(self, email):
        """Validates email format."""
        return email if re.match(r'[^@]+@[^@]+\.[^@]+', email) else None

    def normalize_gender(self, sex):
        """Normalizes gender to 'M', 'F', or 'ND' (not defined)."""
        if isinstance(sex, list) and len(sex) > 0:
            sex = sex[0]
        return sex.upper() if isinstance(sex, str) and sex.upper() in ['M', 'F'] else 'ND'

    def process_and_group_data(self, raw_message):
        """Transforms and groups raw message data into categories."""
        try:
            data = json.loads(raw_message)
        except json.JSONDecodeError as e:
            self.logger.error(f"Error decoding JSON: {e}")
            return {"error": "Invalid JSON format"}

        # Identify data type and return structured data with possible None values
        if "city" in data or "country" in data:
            return {
                "fullname": data.get("fullname", None),
                "address": data.get("address", None),
                "city": data.get("city", None),
                "country": data.get("country", None)
            }
        elif "name" in data or "last_name" in data:
            return {
                "name": data.get("name", None),
                "last_name": data.get("last_name", None),
                "fullname": f"{data.get('name', '')} {data.get('last_name', '')}".strip(),
                "telfnumber": self.standardize_phone(data.get("telfnumber")),
                "passport": data.get("passport", None),
                "email": self.validate_email(data.get("email")),
                "sex": self.normalize_gender(data.get("sex"))
            }
        elif "job" in data or "company" in data:
            return {
                "fullname": data.get("fullname", None),
                "company": data.get("company", None),
                "job": data.get("job", None),
                "company_telfnumber": self.standardize_phone(data.get("company_telfnumber")),
                "company_address": data.get("company_address", None),
                "company_email": self.validate_email(data.get("company_email"))
            }
        elif "IBAN" in data or "salary" in data:
            salary, currency = self.separate_salary_currency(data.get("salary"))
            return {
                "passport": data.get("passport", None),
                "IBAN": data.get("IBAN", None),
                "salary": salary,
                "currency": currency
            }
        elif "IPv4" in data:
            return {
                "address": data.get("address", None),
                "IPv4": data.get("IPv4", None)
            }
        return {"error": "No valid data found"}

    def transform(self, message):
        """Transforms and caches messages in Redis, setting missing fields to None if incomplete."""
        passport = message.get('passport', 'unknown')
        redis_key = f"user:{passport}"
        
        # Load current data from Redis
        cached_data = self.redis_client.get(redis_key)
        user_data = json.loads(cached_data) if cached_data else {}

        # Update user data with the current message
        transformed_message = self.process_and_group_data(json.dumps(message))
        user_data.update({k: v for k, v in transformed_message.items() if v is not None})

        # Save merged data back to Redis
        self.redis_client.set(redis_key, json.dumps(user_data))
        self.logger.info(f"Data for passport {passport} stored in Redis.")
        
        # Return updated data (now stored regardless of completeness)
        return user_data
