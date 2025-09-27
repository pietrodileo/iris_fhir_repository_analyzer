from typing import Dict, List, Any, Optional

class FHIRParser:
    """
    Custom FHIR parser that handles FHIR JSON data without external validation libraries.
    Focuses on extracting meaningful data rather than strict validation.
    """
    
    @staticmethod
    def safe_get(data: Dict, *keys: str, default=None):
        """
        Safely navigate nested dictionaries.
        
        Args:
            data: Dictionary to navigate
            keys: Sequence of keys to follow
            default: Default value if key path doesn't exist
            
        Returns:
            Value at the key path or default
        """
        for key in keys:
            if isinstance(data, dict) and key in data:
                data = data[key]
            else:
                return default
        return data
    
    @staticmethod
    def extract_codeable_concept(cc: Optional[Dict], prefer_text: bool = True) -> Optional[str]:
        """
        Extract meaningful text from FHIR CodeableConcept.
        
        Args:
            cc: CodeableConcept dictionary
            prefer_text: Whether to prefer 'text' field over coding display
            
        Returns:
            String representation of the concept or None
        """
        if not cc or not isinstance(cc, dict):
            return None
        
        # Try text field first if preferred
        if prefer_text and cc.get('text'):
            return cc['text']
        
        # Try coding array
        codings = cc.get('coding', [])
        if isinstance(codings, list):
            for coding in codings:
                if isinstance(coding, dict):
                    # Prefer display, fallback to code
                    display = coding.get('display')
                    if display:
                        return display
                    code = coding.get('code')
                    if code:
                        return code
        
        # Fallback to text if not preferred initially
        if not prefer_text and cc.get('text'):
            return cc['text']
        
        return None
    
    @staticmethod
    def extract_coding_details(cc: Optional[Dict]) -> Dict[str, Any]:
        """
        Extract detailed coding information from CodeableConcept.
        
        Returns:
            Dictionary with system, code, display, and text
        """
        result = {
            'system': None,
            'code': None,
            'display': None,
            'text': None
        }
        
        if not cc or not isinstance(cc, dict):
            return result
        
        result['text'] = cc.get('text')
        
        codings = cc.get('coding', [])
        if isinstance(codings, list) and codings:
            first_coding = codings[0]
            if isinstance(first_coding, dict):
                result['system'] = first_coding.get('system')
                result['code'] = first_coding.get('code')
                result['display'] = first_coding.get('display')
        
        return result
    
    @staticmethod
    def extract_human_name(name_list: Optional[List[Dict]]) -> Dict[str, Any]:
        """
        Extract human name components from FHIR HumanName array.
        
        Returns:
            Dictionary with name components
        """
        result = {
            'family': None,
            'given': [],
            'prefix': [],
            'suffix': [],
            'full_name': None,
            'use': None
        }
        
        if not name_list or not isinstance(name_list, list):
            return result
        
        # Prefer 'official' or 'usual' names, fallback to first
        preferred_name = None
        for name in name_list:
            if isinstance(name, dict):
                use = name.get('use', '').lower()
                if use in ['official', 'usual']:
                    preferred_name = name
                    break
        
        if not preferred_name:
            preferred_name = name_list[0] if isinstance(name_list[0], dict) else {}
        
        result['family'] = preferred_name.get('family')
        result['given'] = preferred_name.get('given', [])
        result['prefix'] = preferred_name.get('prefix', [])
        result['suffix'] = preferred_name.get('suffix', [])
        result['use'] = preferred_name.get('use')
        
        # Build full name
        name_parts = []
        if result['prefix']:
            name_parts.extend(result['prefix'])
        if result['given']:
            name_parts.extend(result['given'])
        if result['family']:
            name_parts.append(result['family'])
        if result['suffix']:
            name_parts.extend(result['suffix'])
        
        result['full_name'] = ' '.join(name_parts) if name_parts else None
        
        return result
    
    @staticmethod
    def extract_telecom(telecom_list: Optional[List[Dict]], system: Optional[str] = None) -> Dict[str, Any]:
        """
        Extract telecom information.
        
        Args:
            telecom_list: List of telecom entries
            system: Specific system to filter by (phone, email, fax, url)
            
        Returns:
            Dictionary with telecom information
        """
        result = {
            'phone': [],
            'email': [],
            'fax': [],
            'url': [],
            'other': []
        }
        
        if not telecom_list or not isinstance(telecom_list, list):
            return result
        
        for telecom in telecom_list:
            if not isinstance(telecom, dict):
                continue
            
            system_type = telecom.get('system', '').lower()
            value = telecom.get('value')
            use = telecom.get('use')
            
            if value:
                entry = {'value': value, 'use': use}
                
                if system_type in result:
                    result[system_type].append(entry)
                else:
                    result['other'].append({**entry, 'system': system_type})
        
        # If specific system requested, return first value
        if system and system in result and result[system]:
            return result[system][0]['value']
        
        return result
    
    @staticmethod
    def extract_address(address_list: Optional[List[Dict]]) -> Dict[str, Any]:
        """
        Extract address information from FHIR Address array.
        
        Returns:
            Dictionary with address components
        """
        result = {
            'line': [],
            'city': None,
            'district': None,
            'state': None,
            'postal_code': None,
            'country': None,
            'full_address': None,
            'use': None,
            'type': None
        }
        
        if not address_list or not isinstance(address_list, list):
            return result
        
        # Prefer 'home' or 'work' addresses, fallback to first
        preferred_address = None
        for addr in address_list:
            if isinstance(addr, dict):
                use = addr.get('use', '').lower()
                if use in ['home', 'work']:
                    preferred_address = addr
                    break
        
        if not preferred_address:
            preferred_address = address_list[0] if isinstance(address_list[0], dict) else {}
        
        result['line'] = preferred_address.get('line', [])
        result['city'] = preferred_address.get('city')
        result['district'] = preferred_address.get('district')
        result['state'] = preferred_address.get('state')
        result['postal_code'] = preferred_address.get('postalCode')
        result['country'] = preferred_address.get('country')
        result['use'] = preferred_address.get('use')
        result['type'] = preferred_address.get('type')
        
        # Build full address
        address_parts = []
        if result['line']:
            address_parts.extend(result['line'])
        if result['city']:
            address_parts.append(result['city'])
        if result['district']:
            address_parts.append(result['district'])
        if result['state']:
            address_parts.append(result['state'])
        if result['postal_code']:
            address_parts.append(result['postal_code'])
        if result['country']:
            address_parts.append(result['country'])
        
        result['full_address'] = ', '.join(address_parts) if address_parts else None
        
        return result
    
    @staticmethod
    def extract_identifiers(identifier_list: Optional[List[Dict]]) -> Dict[str, Any]:
        """
        Extract identifiers from FHIR Identifier array.
        
        Returns:
            Dictionary with MRN, UUID, SSN, DL, Passport and a list of all identifiers.
        """
        result = {
            "mrn": None,
            "uuid": None,
            "ssn": None,
            "driver_license": None,
            "passport": None,
            "all_identifiers": []
        }

        if not identifier_list or not isinstance(identifier_list, list):
            return result

        for ident in identifier_list:
            if not isinstance(ident, dict):
                continue
            system = ident.get("system")
            value = ident.get("value")
            id_type = None

            # Identify type
            if ident.get("type") and "coding" in ident["type"]:
                coding = ident["type"]["coding"][0]
                id_type = coding.get("code")

            # Map known identifiers
            if id_type == "MR":
                result["mrn"] = value
            elif system == "https://github.com/synthetichealth/synthea":
                result["uuid"] = value
            elif id_type == "SB" or "us-ssn" in (system or ""):
                result["ssn"] = value
            elif id_type == "DL":
                result["driver_license"] = value
            elif id_type == "PPN":
                result["passport"] = value

            # Save raw identifier
            result["all_identifiers"].append({
                "system": system,
                "type": id_type,
                "value": value
            })

        return result
    
    @staticmethod
    def extract_quantity(quantity: Optional[Dict]) -> Dict[str, Any]:
        """
        Extract Quantity information.
        
        Returns:
            Dictionary with value, unit, system, and code
        """
        result = {
            'value': None,
            'unit': None,
            'system': None,
            'code': None,
            'comparator': None
        }
        
        if not quantity or not isinstance(quantity, dict):
            return result
        
        result['value'] = quantity.get('value')
        result['unit'] = quantity.get('unit')
        result['system'] = quantity.get('system')
        result['code'] = quantity.get('code')
        result['comparator'] = quantity.get('comparator')
        
        return result
    
    @staticmethod
    def extract_period(period: Optional[Dict]) -> Dict[str, Any]:
        """
        Extract Period information.
        
        Returns:
            Dictionary with start and end dates
        """
        result = {
            'start': None,
            'end': None
        }
        
        if not period or not isinstance(period, dict):
            return result
        
        result['start'] = period.get('start')
        result['end'] = period.get('end')
        
        return result
    
    @staticmethod
    def extract_reference(reference: Optional[Dict]) -> Dict[str, Any]:
        """
        Extract Reference information.
        
        Returns:
            Dictionary with reference, display, and type
        """
        result = {
            'reference': None,
            'display': None,
            'type': None,
            'identifier': None
        }
        
        if not reference or not isinstance(reference, dict):
            return result
        
        result['reference'] = reference.get('reference')
        result['display'] = reference.get('display')
        result['type'] = reference.get('type')
        
        # Extract identifier if present
        identifier = reference.get('identifier')
        if identifier and isinstance(identifier, dict):
            result['identifier'] = identifier.get('value')
        
        return result
    
    @staticmethod
    def extract_dosage(dosage_list: Optional[List[Dict]]) -> Dict[str, Any]:
        """
        Extract dosage information from MedicationRequest.
        
        Returns:
            Dictionary with dosage details
        """
        result = {
            'text': None,
            'timing': None,
            'route': None,
            'method': None,
            'dose_quantity': None,
            'dose_range_low': None,
            'dose_range_high': None,
            'max_dose_per_period': None
        }
        
        if not dosage_list or not isinstance(dosage_list, list):
            return result
        
        # Use first dosage instruction
        dosage = dosage_list[0] if isinstance(dosage_list[0], dict) else {}
        
        result['text'] = dosage.get('text')
        
        # Extract timing
        timing = dosage.get('timing', {})
        if isinstance(timing, dict):
            result['timing'] = timing.get('code', {}).get('text') or str(timing.get('repeat', {}))
        
        # Extract route
        route = dosage.get('route')
        result['route'] = FHIRParser.extract_codeable_concept(route)
        
        # Extract method
        method = dosage.get('method')
        result['method'] = FHIRParser.extract_codeable_concept(method)
        
        # Extract dose
        dose_and_rate = dosage.get('doseAndRate', [])
        if isinstance(dose_and_rate, list) and dose_and_rate:
            dose_rate = dose_and_rate[0]
            
            # Dose quantity
            dose_quantity = dose_rate.get('doseQuantity')
            if dose_quantity:
                qty = FHIRParser.extract_quantity(dose_quantity)
                result['dose_quantity'] = f"{qty['value']} {qty['unit']}" if qty['value'] and qty['unit'] else qty['value']
            
            # Dose range
            dose_range = dose_rate.get('doseRange')
            if dose_range and isinstance(dose_range, dict):
                low = FHIRParser.extract_quantity(dose_range.get('low'))
                high = FHIRParser.extract_quantity(dose_range.get('high'))
                result['dose_range_low'] = low['value']
                result['dose_range_high'] = high['value']
        
        return result
