async def classify_data(data):
    """
        Classifies input data based on the fields present.

        This function checks for the presence of a passport, address, fullname or name/lastname 
        in the input data dict. It returns a tuple with a classification string, the relevant 
        data field and the mutated input data dict.

        The data dict is mutated by:
        - Converting name/lastname into a fullname
        - Converting sex into a one letter string

        Args:
        data (dict): The input data dict

        Returns:
        tuple: A tuple containing:
            - The classification string ('passport', 'address' or 'fullname') 
            - The relevant data field value
            - The mutated input data dict

        Example:
        ```python
        data = {'name': 'John', 'lastname': 'Doe', 'sex': 'Male'}

        classification, value, new_data = classify_data(data)

        print(classification) # 'fullname' 
        print(value) # 'John Doe'
        print(new_data) # {'fullname': 'John Doe', 'sex': 'M'}
        ```
    """
    passport = data.get('passport')
    fullname = data.get('fullname')
    address = data.get('address')
    name = data.get('name')
    lastname = data.get('last_name')
    sex = data.get('sex')

    if passport:
        if name:
            data['sex'] = sex[0] if sex else 'Not specified'
            data['fullname'] = f"{name} {lastname}"
            del data['name']
            del data['last_name']
        return 'passport', passport, data
    elif address:
        return 'address', address, data
    elif fullname:
        return 'fullname', fullname, data