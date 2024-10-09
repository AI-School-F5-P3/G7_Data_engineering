class DataTransformer:
    def transform(self, message):
        return {
            'personal_data': message.get('personal_data', {}),
            'location': message.get('location', {}),
            'professional_data': message.get('professional_data', {}),
            'bank_data': message.get('bank_data', {}),
            'net_data': message.get('net_data', {})
        }