import json
import pandas as pd
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime
from .fhir_parser import FHIRParser
import logging
from datetime import date 

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# https://github.com/smart-on-fhir/generated-sample-data
class FHIRAnalyzer:
    def __init__(self, folder_path: Optional[str] = None, output_path: Optional[str] = None):
        """
        Initialize the FHIR analyzer with the folder containing FHIR JSON files.
        
        Args:
            folder_path (str): Path to the folder containing FHIR JSON files
        """
        # verify if input path exists
        if folder_path:
            self.folder_path = Path(folder_path)
            # verify if input folder exists
            if not self.folder_path.exists():
                raise FileNotFoundError(f"Folder {folder_path} does not exist")
        
        # create output folder if specified
        if output_path:    
            self.output_path = Path(output_path)
            self.output_path.mkdir(parents=True, exist_ok=True)
            self.output_path_info = self.output_path / "info_string"
            self.output_path_info.mkdir(parents=True, exist_ok=True)

        self.parser = FHIRParser()
        
    def analyze_fhir(self, data: str | dict) -> tuple[str, dict]:
        """
        Analyze FHIR data (either JSON string or dict).
        Returns a tuple: (info string, dataframe).
        """

        info = ""
        pat_dict = {}
        patient_name = None
        info = ""
        try:
            # Load JSON if input is a string
            if isinstance(data, str):
                data = json.loads(data)

            if not isinstance(data, dict):
                raise ValueError("Input must be a JSON string or dict")

            # Process FHIR Bundle
            if data.get("resourceType") == "Bundle":
                patient_info = self._process_bundle(data)

                if patient_info:
                    info, pat_dict = self._process_patient_info(patient_info)

                if pat_dict:
                    # Take patient name
                    patient_name = pat_dict['Patient']['full_name']
                    if not patient_name:
                        raise ValueError("Patient name not found")
                else:
                    raise ValueError("DataFrame is empty")
                
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {str(e)}")
        except Exception as e:
            if patient_name:
                logger.error(f"Error processing FHIR data for {patient_name}: {str(e)}")
            else:
                logger.error(f"Error processing FHIR data: {str(e)}")

        return info, pat_dict
        
    def load_and_analyze_all_files(self) -> Dict[str, pd.DataFrame]:
        """
        Load and analyze all FHIR JSON files in the specified folder.
        
        Returns:
            Dict[str, pd.DataFrame]: Dictionary with resource types as keys and DataFrames as values
        """
        json_files = list(self.folder_path.glob("*.json"))
        
        if not json_files:
            logger.warning(f"No JSON files found in {self.folder_path}")
            return {}
        
        logger.info(f"Found {len(json_files)} JSON files to process")
                
        for file_path in json_files:
            try:
                logger.info(f"Processing file: {file_path.name}")
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Process the file based on its structure
                if data.get("resourceType") == "Bundle":
                    patient_info = self._process_bundle(data, str(file_path))
                    if patient_info:
                        info, df = self._process_patient_info(patient_info)
                        
                    if not df.empty:
                        # Prendo il nome del paziente dal df (prima riga Patient.full_name)
                        patient_name = df[df["resource_type"] == "Patient"]["full_name"].iloc[0]
                        if not patient_name:
                            raise ValueError("Patient name not found")
                        
                        # save the df as a csv
                        safe_name = patient_name.replace(" ", "_").replace("/", "_")
                        csv_path = self.output_path / f"{safe_name}.csv"
                        df.to_csv(csv_path, index=False)
                        logger.info(f"Saved DataFrame for patient '{patient_name}' to {csv_path}")

                        # save the info string as a txt file
                        txt_path = self.output_path_info / f"{safe_name}.txt"
                        with open(txt_path, 'w', encoding='utf-8') as f:
                            f.write(info)
                        logger.info(f"Saved info string for patient '{patient_name}' to {txt_path}")
                    
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error in {file_path.name}: {str(e)}")
                continue
            except Exception as e:
                logger.error(f"Error processing {file_path.name}: {str(e)}")
                continue
        
    def _process_bundle(self, bundle_data: Dict, file_path: Optional[str] = None) -> Optional[Dict]:
        """Extract patient data from a FHIR Bundle."""
        
        resource_data = {
            'Patient': [],  
            'Observation': [],
            'Condition': [],
            'Encounter': [],
            'Procedure': [],
            'MedicationRequest': [],
            'DiagnosticReport': [],
            'Practitioner': [],
            'Organization': [],
            'Location': [],
            'Medication': [],
            'AllergyIntolerance': [],
            'Immunization': [],
            'CarePlan': []
        }

        try:
            entries = bundle_data.get("entry", [])
            
            for entry in entries:
                resource = entry.get("resource", {})
                if resource and isinstance(resource, dict):
                    self._categorize_and_extract_resource(resource, resource_data, file_path)
            
            return resource_data
        
        except Exception as e:
            if not file_path:
                logger.error(f"Error extracting patient: {str(e)}")
            else:
                logger.error(f"Error extracting patient from {file_path}: {str(e)}")
            return None
        
    def _categorize_and_extract_resource(self, resource: Dict, resource_data: Dict, file_path: Optional[str] = None):
        """Categorize and extract data from a FHIR resource."""
        resource_type = resource.get('resourceType', 'Unknown')
        
        # Add metadata
        if file_path:
            resource['_source_file'] = Path(file_path).name
        resource['_processed_at'] = datetime.now().isoformat()
        
        # Use extraction methods based on resource type
        extraction_methods = {
            'Patient': self._extract_patient_data,
            'Observation': self._extract_observation_data,
            'Condition': self._extract_condition_data,
            'Encounter': self._extract_encounter_data,
            'Procedure': self._extract_procedure_data,
            'MedicationRequest': self._extract_medication_request_data,
            'DiagnosticReport': self._extract_diagnostic_report_data,
            'Practitioner': self._extract_practitioner_data,
            'Organization': self._extract_organization_data,
            'Location': self._extract_location_data,
            'Medication': self._extract_medication_data,
            'AllergyIntolerance': self._extract_allergy_intolerance_data,
            'Immunization': self._extract_immunization_data,
            'CarePlan': self._extract_care_plan_data
        }
        
        if resource_type in extraction_methods:
            extracted_data = extraction_methods[resource_type](resource)
            resource_data[resource_type].append(extracted_data)
    
    def _build_patient_summaries(self, patient_dict: Dict[str, Dict]) -> Dict[str, str]:
        """Build summary strings for each patient."""
        summaries = {}
        for patient_id, pdata in patient_dict.items():
            name_info = pdata.get("name", {})
            telecom_info = pdata.get("telecom", {})
            address_info = pdata.get("address", {})

            summary = ""
            if name_info.get("full_name"):
                summary += name_info["full_name"]
            if telecom_info.get("phone"):
                summary += f" Phone: {telecom_info['phone'][0]['value']}"
            if telecom_info.get("email"):
                summary += f" Email: {telecom_info['email'][0]['value']}"
            if address_info.get("full_address"):
                summary += f" Address: {address_info['full_address']}"

            summaries[patient_id] = summary
            pdata["_summary"] = summary  # attach also to patient dict

        return summaries
    
    def _extract_patient_data(self, resource: Dict) -> Dict:
        """Extract structured data from Patient resource."""
        name_info = self.parser.extract_human_name(resource.get('name'))
        telecom_info = self.parser.extract_telecom(resource.get('telecom'))
        address_info = self.parser.extract_address(resource.get('address'))
        identifier_info = self.parser.extract_identifiers(resource.get('identifier'))
        
        return {
            'id': resource.get('id'),
            'resourceType': resource.get('resourceType'),
            'active': resource.get('active'),
            'family_name': name_info['family'],
            'given_names': ' '.join(name_info['given']) if name_info['given'] else None,
            'full_name': name_info['full_name'],
            'name_use': name_info['use'],
            'gender': resource.get('gender'),
            'birth_date': resource.get('birthDate'),
            'deceased_datetime': resource.get('deceasedDateTime'),
            'marital_status': self.parser.extract_codeable_concept(resource.get('maritalStatus')),
            'phone': telecom_info['phone'][0]['value'] if telecom_info['phone'] else None,
            'email': telecom_info['email'][0]['value'] if telecom_info['email'] else None,
            'address_full': address_info['full_address'],
            'address_city': address_info['city'],
            'address_state': address_info['state'],
            'address_postal_code': address_info['postal_code'],
            'address_country': address_info['country'],
            'language': resource.get('communication', [{}])[0].get('language', {}).get('text') if resource.get('communication') else None,
            'managing_organization': self.parser.extract_reference(resource.get('managingOrganization')).get('reference'),
            'ssn_id': identifier_info['ssn'],
            'mrn_id': identifier_info['mrn'],
            'uuid_id': identifier_info['uuid'],
            'driver_license': identifier_info['driver_license'],
            'passport_number': identifier_info['passport'],
            '_source_file': resource.get('_source_file'),
            '_processed_at': resource.get('_processed_at')
        }
    
    def _extract_observation_data(self, resource: Dict) -> Dict:
        """Extract structured data from Observation resource."""
        # Handle different value types
        value_data = {}
        for value_type in ['valueQuantity', 'valueCodeableConcept', 'valueString', 'valueBoolean', 'valueInteger', 'valueRange']:
            if value_type in resource:
                if value_type == 'valueQuantity':
                    qty = self.parser.extract_quantity(resource[value_type])
                    value_data.update({
                        'value_quantity': qty['value'],
                        'value_unit': qty['unit'],
                        'value_comparator': qty['comparator']
                    })
                elif value_type == 'valueCodeableConcept':
                    value_data['value_codeable_concept'] = self.parser.extract_codeable_concept(resource[value_type])
                else:
                    value_data[value_type.replace('value', 'value_').lower()] = resource[value_type]
                break
        
        # Extract components for complex observations
        components = []
        if resource.get('component'):
            for comp in resource['component']:
                comp_code = self.parser.extract_codeable_concept(comp.get('code'))
                comp_value = None
                for val_type in ['valueQuantity', 'valueCodeableConcept', 'valueString']:
                    if val_type in comp:
                        if val_type == 'valueQuantity':
                            qty = self.parser.extract_quantity(comp[val_type])
                            comp_value = f"{qty['value']} {qty['unit']}" if qty['value'] and qty['unit'] else qty['value']
                        elif val_type == 'valueCodeableConcept':
                            comp_value = self.parser.extract_codeable_concept(comp[val_type])
                        else:
                            comp_value = comp[val_type]
                        break
                
                if comp_code and comp_value:
                    components.append(f"{comp_code}: {comp_value}")
        
        return {
            'id': resource.get('id'),
            'resourceType': resource.get('resourceType'),
            'status': resource.get('status'),
            'category': self.parser.extract_codeable_concept(resource.get('category', [{}])[0] if resource.get('category') else None),
            'code': self.parser.extract_codeable_concept(resource.get('code')),
            'code_system': self.parser.extract_coding_details(resource.get('code')).get('system'),
            'code_code': self.parser.extract_coding_details(resource.get('code')).get('code'),
            'subject_reference': self.parser.extract_reference(resource.get('subject')).get('reference'),
            'encounter_reference': self.parser.extract_reference(resource.get('encounter')).get('reference'),
            'effective_datetime': resource.get('effectiveDateTime'),
            'effective_period_start': self.parser.extract_period(resource.get('effectivePeriod')).get('start'),
            'effective_period_end': self.parser.extract_period(resource.get('effectivePeriod')).get('end'),
            'issued': resource.get('issued'),
            'performer_reference': self.parser.extract_reference(resource.get('performer', [{}])[0] if resource.get('performer') else None).get('reference'),
            **value_data,
            'interpretation': self.parser.extract_codeable_concept(resource.get('interpretation', [{}])[0] if resource.get('interpretation') else None),
            'note': resource.get('note', [{}])[0].get('text') if resource.get('note') else None,
            'body_site': self.parser.extract_codeable_concept(resource.get('bodySite')),
            'method': self.parser.extract_codeable_concept(resource.get('method')),
            'components': ' | '.join(components) if components else None,
            'reference_range_low': resource.get('referenceRange', [{}])[0].get('low', {}).get('value') if resource.get('referenceRange') else None,
            'reference_range_high': resource.get('referenceRange', [{}])[0].get('high', {}).get('value') if resource.get('referenceRange') else None,
            'reference_range_text': resource.get('referenceRange', [{}])[0].get('text') if resource.get('referenceRange') else None,
            '_source_file': resource.get('_source_file'),
            '_processed_at': resource.get('_processed_at')
        }
    
    def _extract_condition_data(self, resource: Dict) -> Dict:
        """Extract structured data from Condition resource."""
        onset_data = {}
        if 'onsetDateTime' in resource:
            onset_data['onset_datetime'] = resource['onsetDateTime']
        elif 'onsetAge' in resource:
            age = self.parser.extract_quantity(resource['onsetAge'])
            onset_data['onset_age'] = f"{age['value']} {age['unit']}" if age['value'] and age['unit'] else age['value']
        elif 'onsetPeriod' in resource:
            period = self.parser.extract_period(resource['onsetPeriod'])
            onset_data['onset_period_start'] = period['start']
            onset_data['onset_period_end'] = period['end']
        elif 'onsetString' in resource:
            onset_data['onset_string'] = resource['onsetString']
        
        return {
            'id': resource.get('id'),
            'resourceType': resource.get('resourceType'),
            'clinical_status': self.parser.extract_codeable_concept(resource.get('clinicalStatus')),
            'verification_status': self.parser.extract_codeable_concept(resource.get('verificationStatus')),
            "assertedDate": resource.get('assertedDate'),
            'category': self.parser.extract_codeable_concept(resource.get('category', [{}])[0] if resource.get('category') else None),
            'severity': self.parser.extract_codeable_concept(resource.get('severity')),
            'code': self.parser.extract_codeable_concept(resource.get('code')),
            'code_system': self.parser.extract_coding_details(resource.get('code')).get('system'),
            'code_code': self.parser.extract_coding_details(resource.get('code')).get('code'),
            'body_site': self.parser.extract_codeable_concept(resource.get('bodySite', [{}])[0] if resource.get('bodySite') else None),
            'subject_reference': self.parser.extract_reference(resource.get('subject')).get('reference'),
            'encounter_reference': self.parser.extract_reference(resource.get('encounter')).get('reference'),
            **onset_data,
            'abatement_datetime': resource.get('abatementDateTime'),
            'recorded_date': resource.get('recordedDate'),
            'recorder_reference': self.parser.extract_reference(resource.get('recorder')).get('reference'),
            'asserter_reference': self.parser.extract_reference(resource.get('asserter')).get('reference'),
            'stage_summary': self.parser.extract_codeable_concept(resource.get('stage', {}).get('summary')) if resource.get('stage') else None,
            'evidence': self.parser.extract_codeable_concept(resource.get('evidence', [{}])[0].get('code', [{}])[0] if resource.get('evidence') else None),
            'note': resource.get('note', [{}])[0].get('text') if resource.get('note') else None,
            '_source_file': resource.get('_source_file'),
            '_processed_at': resource.get('_processed_at')
        }
    
    def _extract_encounter_data(self, resource: Dict) -> Dict:
        """Extract structured data from Encounter resource."""
        period_info = self.parser.extract_period(resource.get('period'))
        length_info = self.parser.extract_quantity(resource.get('length'))
        
        # Extract participant information
        participants = []
        if resource.get('participant'):
            for participant in resource['participant']:
                participant_type = self.parser.extract_codeable_concept(participant.get('type', [{}])[0] if participant.get('type') else None)
                participant_ref = self.parser.extract_reference(participant.get('individual')).get('reference')
                if participant_type and participant_ref:
                    participants.append(f"{participant_type}: {participant_ref}")
        
        return {
            'id': resource.get('id'),
            'resourceType': resource.get('resourceType'),
            'status': resource.get('status'),
            'status_history': resource.get('statusHistory', [{}])[-1].get('status') if resource.get('statusHistory') else None,
            'class_code': resource.get('class', {}).get('code'),
            'class_display': resource.get('class', {}).get('display'),
            'type': self.parser.extract_codeable_concept(resource.get('type', [{}])[0] if resource.get('type') else None),
            'service_type': self.parser.extract_codeable_concept(resource.get('serviceType')),
            'priority': self.parser.extract_codeable_concept(resource.get('priority')),
            'subject_reference': self.parser.extract_reference(resource.get('subject')).get('reference'),
            'episode_of_care_reference': self.parser.extract_reference(resource.get('episodeOfCare', [{}])[0] if resource.get('episodeOfCare') else None).get('reference'),
            'period_start': period_info['start'],
            'period_end': period_info['end'],
            'length_value': length_info['value'],
            'length_unit': length_info['unit'],
            'reason_code': self.parser.extract_codeable_concept(resource.get('reasonCode', [{}])[0] if resource.get('reasonCode') else None),
            'reason_reference': self.parser.extract_reference(resource.get('reasonReference', [{}])[0] if resource.get('reasonReference') else None).get('reference'),
            'diagnosis': self.parser.extract_codeable_concept(resource.get('diagnosis', [{}])[0].get('condition') if resource.get('diagnosis') else None),
            'hospitalization_admit_source': self.parser.extract_codeable_concept(resource.get('hospitalization', {}).get('admitSource')) if resource.get('hospitalization') else None,
            'hospitalization_discharge_disposition': self.parser.extract_codeable_concept(resource.get('hospitalization', {}).get('dischargeDisposition')) if resource.get('hospitalization') else None,
            'location_reference': self.parser.extract_reference(resource.get('location', [{}])[0].get('location') if resource.get('location') else None).get('reference'),
            'service_provider_reference': self.parser.extract_reference(resource.get('serviceProvider')).get('reference'),
            'participants': ' | '.join(participants) if participants else None,
            '_source_file': resource.get('_source_file'),
            '_processed_at': resource.get('_processed_at')
        }
    
    def _extract_procedure_data(self, resource: Dict) -> Dict:
        """Extract structured data from Procedure resource."""
        performed_data = {}
        if 'performedDateTime' in resource:
            performed_data['performed_datetime'] = resource['performedDateTime']
        elif 'performedPeriod' in resource:
            period = self.parser.extract_period(resource['performedPeriod'])
            performed_data['performed_period_start'] = period['start']
            performed_data['performed_period_end'] = period['end']
        elif 'performedString' in resource:
            performed_data['performed_string'] = resource['performedString']
        elif 'performedAge' in resource:
            age = self.parser.extract_quantity(resource['performedAge'])
            performed_data['performed_age'] = f"{age['value']} {age['unit']}" if age['value'] and age['unit'] else age['value']
        
        # Extract performer information
        performers = []
        if resource.get('performer'):
            for performer in resource['performer']:
                function = self.parser.extract_codeable_concept(performer.get('function'))
                actor_ref = self.parser.extract_reference(performer.get('actor')).get('reference')
                if actor_ref:
                    performers.append(f"{function}: {actor_ref}" if function else actor_ref)
        
        return {
            'id': resource.get('id'),
            'resourceType': resource.get('resourceType'),
            'status': resource.get('status'),
            'status_reason': self.parser.extract_codeable_concept(resource.get('statusReason')),
            'category': self.parser.extract_codeable_concept(resource.get('category')),
            'code': self.parser.extract_codeable_concept(resource.get('code')),
            'code_system': self.parser.extract_coding_details(resource.get('code')).get('system'),
            'code_code': self.parser.extract_coding_details(resource.get('code')).get('code'),
            'subject_reference': self.parser.extract_reference(resource.get('subject')).get('reference'),
            'encounter_reference': self.parser.extract_reference(resource.get('encounter')).get('reference'),
            **performed_data,
            'recorder_reference': self.parser.extract_reference(resource.get('recorder')).get('reference'),
            'asserter_reference': self.parser.extract_reference(resource.get('asserter')).get('reference'),
            'performers': ' | '.join(performers) if performers else None,
            'note': resource.get('note', [{}])[0].get('text') if resource.get('note') else None,
            'reason_code': self.parser.extract_codeable_concept(resource.get('reasonCode', [{}])[0] if resource.get('reasonCode') else None),
            'is_subpotent': resource.get('isSubpotent'),
            'subpotent_reason': self.parser.extract_codeable_concept(resource.get('subpotentReason', [{}])[0] if resource.get('subpotentReason') else None),
            'education_document_type': resource.get('education', [{}])[0].get('documentType') if resource.get('education') else None,
            'education_reference': resource.get('education', [{}])[0].get('reference') if resource.get('education') else None,
            'education_publication_date': resource.get('education', [{}])[0].get('publicationDate') if resource.get('education') else None,
            'program_eligibility': self.parser.extract_codeable_concept(resource.get('programEligibility', [{}])[0] if resource.get('programEligibility') else None),
            'funding_source': self.parser.extract_codeable_concept(resource.get('fundingSource')),
            # 'reactions': ' | '.join(reactions) if reactions else None,
            # 'protocols_applied': ' | '.join(protocols) if protocols else None,
            '_source_file': resource.get('_source_file'),
            '_processed_at': resource.get('_processed_at')
        }
    
    def _extract_care_plan_data(self, resource: Dict) -> Dict:
        """Extract structured data from CarePlan resource."""
        period_info = self.parser.extract_period(resource.get('period'))
        
        # Extract care team
        care_team = []
        if resource.get('careTeam'):
            for team_ref in resource['careTeam']:
                ref = self.parser.extract_reference(team_ref).get('reference')
                if ref:
                    care_team.append(ref)
        
        # Extract addresses (conditions/observations being addressed)
        addresses = []
        if resource.get('addresses'):
            for address_ref in resource['addresses']:
                ref = self.parser.extract_reference(address_ref).get('reference')
                if ref:
                    addresses.append(ref)
        
        # Extract goals
        goals = []
        if resource.get('goal'):
            for goal_ref in resource['goal']:
                ref = self.parser.extract_reference(goal_ref).get('reference')
                if ref:
                    goals.append(ref)
        
        # Extract activities
        activities = []
        if resource.get('activity'):
            for activity in resource['activity']:
                outcome_cc = self.parser.extract_codeable_concept(activity.get('outcomeCodeableConcept', [{}])[0] if activity.get('outcomeCodeableConcept') else None)
                outcome_ref = self.parser.extract_reference(activity.get('outcomeReference', [{}])[0] if activity.get('outcomeReference') else None).get('reference')
                progress = activity.get('progress', [{}])[0].get('text') if activity.get('progress') else None
                reference = self.parser.extract_reference(activity.get('reference')).get('reference')
                
                detail = activity.get('detail', {})
                activity_parts = []
                
                if reference:
                    activity_parts.append(f"Reference: {reference}")
                elif detail:
                    kind = detail.get('kind')
                    code = self.parser.extract_codeable_concept(detail.get('code'))
                    status = detail.get('status')
                    description = detail.get('description')
                    
                    if kind:
                        activity_parts.append(f"Kind: {kind}")
                    if code:
                        activity_parts.append(f"Code: {code}")
                    if status:
                        activity_parts.append(f"Status: {status}")
                    if description:
                        activity_parts.append(f"Description: {description}")
                
                if outcome_cc:
                    activity_parts.append(f"Outcome: {outcome_cc}")
                elif outcome_ref:
                    activity_parts.append(f"Outcome: {outcome_ref}")
                
                if progress:
                    activity_parts.append(f"Progress: {progress}")
                
                if activity_parts:
                    activities.append('; '.join(activity_parts))
        
        return {
            'id': resource.get('id'),
            'resourceType': resource.get('resourceType'),
            'status': resource.get('status'),
            'intent': resource.get('intent'),
            'category': self.parser.extract_codeable_concept(resource.get('category', [{}])[0] if resource.get('category') else None),
            'title': resource.get('title'),
            'description': resource.get('description'),
            'subject_reference': self.parser.extract_reference(resource.get('subject')).get('reference'),
            'encounter_reference': self.parser.extract_reference(resource.get('encounter')).get('reference'),
            'period_start': period_info['start'],
            'period_end': period_info['end'],
            'created': resource.get('created'),
            'author_reference': self.parser.extract_reference(resource.get('author')).get('reference'),
            'contributor_reference': self.parser.extract_reference(resource.get('contributor', [{}])[0] if resource.get('contributor') else None).get('reference'),
            'care_team': ' | '.join(care_team) if care_team else None,
            'addresses': ' | '.join(addresses) if addresses else None,
            'supporting_info': self.parser.extract_reference(resource.get('supportingInfo', [{}])[0] if resource.get('supportingInfo') else None).get('reference'),
            'goals': ' | '.join(goals) if goals else None,
            'activities': ' | '.join(activities) if activities else None,
            'note': resource.get('note', [{}])[0].get('text') if resource.get('note') else None,
            '_source_file': resource.get('_source_file'),
            '_processed_at': resource.get('_processed_at')
        }
    
    def _extract_medication_request_data(self, resource: Dict) -> Dict:
        """Extract structured data from MedicationRequest resource."""
        dosage_info = self.parser.extract_dosage(resource.get('dosageInstruction'))
        
        # Extract medication information
        medication_data = {}
        if 'medicationCodeableConcept' in resource:
            medication_data['medication_codeable_concept'] = self.parser.extract_codeable_concept(resource['medicationCodeableConcept'])
            medication_data['medication_system'] = self.parser.extract_coding_details(resource['medicationCodeableConcept']).get('system')
            medication_data['medication_code'] = self.parser.extract_coding_details(resource['medicationCodeableConcept']).get('code')
        elif 'medicationReference' in resource:
            medication_data['medication_reference'] = self.parser.extract_reference(resource['medicationReference']).get('reference')
        
        # Extract dispense request
        dispense_request = resource.get('dispenseRequest', {})
        
        return {
            'id': resource.get('id'),
            'resourceType': resource.get('resourceType'),
            'status': resource.get('status'),
            'status_reason': self.parser.extract_codeable_concept(resource.get('statusReason')),
            'intent': resource.get('intent'),
            'category': self.parser.extract_codeable_concept(resource.get('category', [{}])[0] if resource.get('category') else None),
            'priority': resource.get('priority'),
            **medication_data,
            'subject_reference': self.parser.extract_reference(resource.get('subject')).get('reference'),
            'encounter_reference': self.parser.extract_reference(resource.get('encounter')).get('reference'),
            'authored_on': resource.get('authoredOn'),
            'requester_reference': self.parser.extract_reference(resource.get('requester')).get('reference'),
            'performer_reference': self.parser.extract_reference(resource.get('performer')).get('reference'),
            'performer_type': self.parser.extract_codeable_concept(resource.get('performerType')),
            'recorder_reference': self.parser.extract_reference(resource.get('recorder')).get('reference'),
            'reason_code': self.parser.extract_codeable_concept(resource.get('reasonCode', [{}])[0] if resource.get('reasonCode') else None),
            'reason_reference': self.parser.extract_reference(resource.get('reasonReference', [{}])[0] if resource.get('reasonReference') else None).get('reference'),
            'course_of_therapy_type': self.parser.extract_codeable_concept(resource.get('courseOfTherapyType')),
            'dosage_text': dosage_info['text'],
            'dosage_timing': dosage_info['timing'],
            'dosage_route': dosage_info['route'],
            'dosage_method': dosage_info['method'],
            'dosage_dose_quantity': dosage_info['dose_quantity'],
            'dosage_dose_range_low': dosage_info['dose_range_low'],
            'dosage_dose_range_high': dosage_info['dose_range_high'],
            'dispense_quantity': self.parser.extract_quantity(dispense_request.get('quantity')).get('value') if dispense_request.get('quantity') else None,
            'dispense_days_supply': self.parser.extract_quantity(dispense_request.get('expectedSupplyDuration')).get('value') if dispense_request.get('expectedSupplyDuration') else None,
            'dispense_number_repeats': dispense_request.get('numberOfRepeatsAllowed'),
            'substitution_allowed': resource.get('substitution', {}).get('allowedBoolean'),
            'note': resource.get('note', [{}])[0].get('text') if resource.get('note') else None,
            '_source_file': resource.get('_source_file'),
            '_processed_at': resource.get('_processed_at')
        }
    
    def _extract_diagnostic_report_data(self, resource: Dict) -> Dict:
        """Extract structured data from DiagnosticReport resource."""
        effective_data = {}
        if 'effectiveDateTime' in resource:
            effective_data['effective_datetime'] = resource['effectiveDateTime']
        elif 'effectivePeriod' in resource:
            period = self.parser.extract_period(resource['effectivePeriod'])
            effective_data['effective_period_start'] = period['start']
            effective_data['effective_period_end'] = period['end']
        
        # Extract result references
        results = []
        if resource.get('result'):
            for result in resource['result']:
                ref = self.parser.extract_reference(result).get('reference')
                if ref:
                    results.append(ref)
        
        # Extract media/images
        media = []
        if resource.get('media'):
            for m in resource['media']:
                comment = m.get('comment')
                link = self.parser.extract_reference(m.get('link')).get('reference')
                if link:
                    media.append(f"{comment}: {link}" if comment else link)
        
        return {
            'id': resource.get('id'),
            'resourceType': resource.get('resourceType'),
            'status': resource.get('status'),
            'category': self.parser.extract_codeable_concept(resource.get('category', [{}])[0] if resource.get('category') else None),
            'code': self.parser.extract_codeable_concept(resource.get('code')),
            'code_system': self.parser.extract_coding_details(resource.get('code')).get('system'),
            'code_code': self.parser.extract_coding_details(resource.get('code')).get('code'),
            'subject_reference': self.parser.extract_reference(resource.get('subject')).get('reference'),
            'encounter_reference': self.parser.extract_reference(resource.get('encounter')).get('reference'),
            **effective_data,
            'issued': resource.get('issued'),
            'performer_reference': self.parser.extract_reference(resource.get('performer', [{}])[0] if resource.get('performer') else None).get('reference'),
            'results_interpreter_reference': self.parser.extract_reference(resource.get('resultsInterpreter', [{}])[0] if resource.get('resultsInterpreter') else None).get('reference'),
            'specimen_reference': self.parser.extract_reference(resource.get('specimen', [{}])[0] if resource.get('specimen') else None).get('reference'),
            'result_references': ' | '.join(results) if results else None,
            'imaging_study_reference': self.parser.extract_reference(resource.get('imagingStudy', [{}])[0] if resource.get('imagingStudy') else None).get('reference'),
            'media': ' | '.join(media) if media else None,
            'conclusion': resource.get('conclusion'),
            'conclusion_code': self.parser.extract_codeable_concept(resource.get('conclusionCode', [{}])[0] if resource.get('conclusionCode') else None),
            'presented_form_title': resource.get('presentedForm', [{}])[0].get('title') if resource.get('presentedForm') else None,
            'presented_form_creation': resource.get('presentedForm', [{}])[0].get('creation') if resource.get('presentedForm') else None,
            '_source_file': resource.get('_source_file'),
            '_processed_at': resource.get('_processed_at')
        }
    
    def _extract_practitioner_data(self, resource: Dict) -> Dict:
        """Extract structured data from Practitioner resource."""
        name_info = self.parser.extract_human_name(resource.get('name'))
        telecom_info = self.parser.extract_telecom(resource.get('telecom'))
        address_info = self.parser.extract_address(resource.get('address'))
        
        # Extract qualifications
        qualifications = []
        if resource.get('qualification'):
            for qual in resource['qualification']:
                code = self.parser.extract_codeable_concept(qual.get('code'))
                issuer = self.parser.extract_reference(qual.get('issuer')).get('display') or self.parser.extract_reference(qual.get('issuer')).get('reference')
                period = self.parser.extract_period(qual.get('period'))
                
                qual_text = code
                if issuer:
                    qual_text += f" ({issuer})"
                if period['start']:
                    qual_text += f" from {period['start']}"
                
                qualifications.append(qual_text)
        
        return {
            'id': resource.get('id'),
            'resourceType': resource.get('resourceType'),
            'active': resource.get('active'),
            'family_name': name_info['family'],
            'given_names': ' '.join(name_info['given']) if name_info['given'] else None,
            'full_name': name_info['full_name'],
            'prefix': ' '.join(name_info['prefix']) if name_info['prefix'] else None,
            'suffix': ' '.join(name_info['suffix']) if name_info['suffix'] else None,
            'name_use': name_info['use'],
            'gender': resource.get('gender'),
            'birth_date': resource.get('birthDate'),
            'phone': telecom_info['phone'][0]['value'] if telecom_info['phone'] else None,
            'email': telecom_info['email'][0]['value'] if telecom_info['email'] else None,
            'address_full': address_info['full_address'],
            'address_city': address_info['city'],
            'address_state': address_info['state'],
            'address_postal_code': address_info['postal_code'],
            'address_country': address_info['country'],
            'qualifications': ' | '.join(qualifications) if qualifications else None,
            'communication': self.parser.extract_codeable_concept(resource.get('communication', [{}])[0] if resource.get('communication') else None),
            '_source_file': resource.get('_source_file'),
            '_processed_at': resource.get('_processed_at')
        }
    
    def _extract_organization_data(self, resource: Dict) -> Dict:
        """Extract structured data from Organization resource."""
        telecom_info = self.parser.extract_telecom(resource.get('telecom'))
        address_info = self.parser.extract_address(resource.get('address'))
        
        # Extract contact information
        contacts = []
        if resource.get('contact'):
            for contact in resource['contact']:
                purpose = self.parser.extract_codeable_concept(contact.get('purpose'))
                name = self.parser.extract_human_name(contact.get('name')).get('full_name') if contact.get('name') else None
                telecom = self.parser.extract_telecom(contact.get('telecom'))
                
                contact_text = []
                if purpose:
                    contact_text.append(f"Purpose: {purpose}")
                if name:
                    contact_text.append(f"Name: {name}")
                if telecom['phone']:
                    contact_text.append(f"Phone: {telecom['phone'][0]['value']}")
                if telecom['email']:
                    contact_text.append(f"Email: {telecom['email'][0]['value']}")
                
                if contact_text:
                    contacts.append('; '.join(contact_text))
        
        return {
            'id': resource.get('id'),
            'resourceType': resource.get('resourceType'),
            'active': resource.get('active'),
            'name': resource.get('name'),
            'alias': ' | '.join(resource.get('alias', [])),
            'type': self.parser.extract_codeable_concept(resource.get('type', [{}])[0] if resource.get('type') else None),
            'phone': telecom_info['phone'][0]['value'] if telecom_info['phone'] else None,
            'email': telecom_info['email'][0]['value'] if telecom_info['email'] else None,
            'website': telecom_info['url'][0]['value'] if telecom_info['url'] else None,
            'fax': telecom_info['fax'][0]['value'] if telecom_info['fax'] else None,
            'address_full': address_info['full_address'],
            'address_city': address_info['city'],
            'address_state': address_info['state'],
            'address_postal_code': address_info['postal_code'],
            'address_country': address_info['country'],
            'part_of_reference': self.parser.extract_reference(resource.get('partOf')).get('reference'),
            'contacts': ' | '.join(contacts) if contacts else None,
            'endpoint_reference': self.parser.extract_reference(resource.get('endpoint', [{}])[0] if resource.get('endpoint') else None).get('reference'),
            '_source_file': resource.get('_source_file'),
            '_processed_at': resource.get('_processed_at')
        }
    
    def _extract_location_data(self, resource: Dict) -> Dict:
        """Extract structured data from Location resource."""
        telecom_info = self.parser.extract_telecom(resource.get('telecom'))
        address_info = self.parser.extract_address(resource.get('address'))
        position = resource.get('position', {})
        
        return {
            'id': resource.get('id'),
            'resourceType': resource.get('resourceType'),
            'status': resource.get('status'),
            'operational_status': self.parser.extract_codeable_concept(resource.get('operationalStatus')),
            'name': resource.get('name'),
            'alias': ' | '.join(resource.get('alias', [])),
            'description': resource.get('description'),
            'mode': resource.get('mode'),
            'type': self.parser.extract_codeable_concept(resource.get('type', [{}])[0] if resource.get('type') else None),
            'phone': telecom_info['phone'][0]['value'] if telecom_info['phone'] else None,
            'email': telecom_info['email'][0]['value'] if telecom_info['email'] else None,
            'website': telecom_info['url'][0]['value'] if telecom_info['url'] else None,
            'address_full': address_info['full_address'],
            'address_city': address_info['city'],
            'address_state': address_info['state'],
            'address_postal_code': address_info['postal_code'],
            'address_country': address_info['country'],
            'physical_type': self.parser.extract_codeable_concept(resource.get('physicalType')),
            'position_longitude': position.get('longitude'),
            'position_latitude': position.get('latitude'),
            'position_altitude': position.get('altitude'),
            'managing_organization_reference': self.parser.extract_reference(resource.get('managingOrganization')).get('reference'),
            'part_of_reference': self.parser.extract_reference(resource.get('partOf')).get('reference'),
            'hours_of_operation': str(resource.get('hoursOfOperation')) if resource.get('hoursOfOperation') else None,
            'availability_exceptions': resource.get('availabilityExceptions'),
            '_source_file': resource.get('_source_file'),
            '_processed_at': resource.get('_processed_at')
        }
    
    def _extract_medication_data(self, resource: Dict) -> Dict:
        """Extract structured data from Medication resource."""
        # Extract ingredients
        ingredients = []
        if resource.get('ingredient'):
            for ingredient in resource['ingredient']:
                item = ingredient.get('itemCodeableConcept') or ingredient.get('itemReference')
                if ingredient.get('itemCodeableConcept'):
                    item_text = self.parser.extract_codeable_concept(item)
                elif ingredient.get('itemReference'):
                    item_text = self.parser.extract_reference(item).get('reference')
                else:
                    item_text = None
                
                strength = ingredient.get('strength')
                if strength and item_text:
                    numerator = self.parser.extract_quantity(strength.get('numerator'))
                    denominator = self.parser.extract_quantity(strength.get('denominator'))
                    
                    strength_text = ""
                    if numerator['value'] and numerator['unit']:
                        strength_text = f"{numerator['value']} {numerator['unit']}"
                    if denominator['value'] and denominator['unit']:
                        strength_text += f" per {denominator['value']} {denominator['unit']}"
                    
                    ingredients.append(f"{item_text} ({strength_text})" if strength_text else item_text)
                elif item_text:
                    ingredients.append(item_text)
        
        # Extract batch information
        batch_info = resource.get('batch', {})
        
        return {
            'id': resource.get('id'),
            'resourceType': resource.get('resourceType'),
            'status': resource.get('status'),
            'code': self.parser.extract_codeable_concept(resource.get('code')),
            'code_system': self.parser.extract_coding_details(resource.get('code')).get('system'),
            'code_code': self.parser.extract_coding_details(resource.get('code')).get('code'),
            'manufacturer_reference': self.parser.extract_reference(resource.get('manufacturer')).get('reference'),
            'form': self.parser.extract_codeable_concept(resource.get('form')),
            'amount_numerator_value': self.parser.extract_quantity(resource.get('amount', {}).get('numerator')).get('value') if resource.get('amount') else None,
            'amount_numerator_unit': self.parser.extract_quantity(resource.get('amount', {}).get('numerator')).get('unit') if resource.get('amount') else None,
            'amount_denominator_value': self.parser.extract_quantity(resource.get('amount', {}).get('denominator')).get('value') if resource.get('amount') else None,
            'amount_denominator_unit': self.parser.extract_quantity(resource.get('amount', {}).get('denominator')).get('unit') if resource.get('amount') else None,
            'ingredients': ' | '.join(ingredients) if ingredients else None,
            'batch_lot_number': batch_info.get('lotNumber'),
            'batch_expiration_date': batch_info.get('expirationDate'),
            '_source_file': resource.get('_source_file'),
            '_processed_at': resource.get('_processed_at')
        }
    
    def _extract_allergy_intolerance_data(self, resource: Dict) -> Dict:
        """Extract structured data from AllergyIntolerance resource."""
        # Extract reactions
        reactions = []
        if resource.get('reaction'):
            for reaction in resource['reaction']:
                substance = self.parser.extract_codeable_concept(reaction.get('substance'))
                manifestations = []
                if reaction.get('manifestation'):
                    for manifestation in reaction['manifestation']:
                        manifestations.append(self.parser.extract_codeable_concept(manifestation))
                
                severity = reaction.get('severity')
                exposure_route = self.parser.extract_codeable_concept(reaction.get('exposureRoute'))
                
                reaction_parts = []
                if substance:
                    reaction_parts.append(f"Substance: {substance}")
                if manifestations:
                    reaction_parts.append(f"Manifestations: {', '.join(filter(None, manifestations))}")
                if severity:
                    reaction_parts.append(f"Severity: {severity}")
                if exposure_route:
                    reaction_parts.append(f"Route: {exposure_route}")
                
                if reaction_parts:
                    reactions.append('; '.join(reaction_parts))
        
        return {
            'id': resource.get('id'),
            'resourceType': resource.get('resourceType'),
            'clinical_status': self.parser.extract_codeable_concept(resource.get('clinicalStatus')),
            'verification_status': self.parser.extract_codeable_concept(resource.get('verificationStatus')),
            "assertedDate": resource.get('assertedDate'),
            'type': resource.get('type'),
            'category': ' | '.join(resource.get('category', [])),
            'criticality': resource.get('criticality'),
            'code': self.parser.extract_codeable_concept(resource.get('code')),
            'code_system': self.parser.extract_coding_details(resource.get('code')).get('system'),
            'code_code': self.parser.extract_coding_details(resource.get('code')).get('code'),
            'patient_reference': self.parser.extract_reference(resource.get('patient')).get('reference'),
            'encounter_reference': self.parser.extract_reference(resource.get('encounter')).get('reference'),
            'onset_datetime': resource.get('onsetDateTime'),
            'onset_age': self.parser.extract_quantity(resource.get('onsetAge')).get('value') if resource.get('onsetAge') else None,
            'onset_period_start': self.parser.extract_period(resource.get('onsetPeriod')).get('start') if resource.get('onsetPeriod') else None,
            'onset_period_end': self.parser.extract_period(resource.get('onsetPeriod')).get('end') if resource.get('onsetPeriod') else None,
            'onset_range_low': self.parser.extract_quantity(resource.get('onsetRange', {}).get('low')).get('value') if resource.get('onsetRange') else None,
            'onset_range_high': self.parser.extract_quantity(resource.get('onsetRange', {}).get('high')).get('value') if resource.get('onsetRange') else None,
            'onset_string': resource.get('onsetString'),
            'recorded_date': resource.get('recordedDate'),
            'recorder_reference': self.parser.extract_reference(resource.get('recorder')).get('reference'),
            'asserter_reference': self.parser.extract_reference(resource.get('asserter')).get('reference'),
            'last_occurrence': resource.get('lastOccurrence'),
            'note': resource.get('note', [{}])[0].get('text') if resource.get('note') else None,
            'reactions': ' | '.join(reactions) if reactions else None,
            '_source_file': resource.get('_source_file'),
            '_processed_at': resource.get('_processed_at')
        }
    
    def _extract_immunization_data(self, resource: Dict) -> Dict:
        """Extract structured data from Immunization resource."""
        # Extract performers
        performers = []
        if resource.get('performer'):
            for performer in resource['performer']:
                function = self.parser.extract_codeable_concept(performer.get('function'))
                actor_ref = self.parser.extract_reference(performer.get('actor')).get('reference')
                if actor_ref:
                    performers.append(f"{function}: {actor_ref}" if function else actor_ref)
        
        # Extract reactions
        reactions = []
        if resource.get('reaction'):
            for reaction in resource['reaction']:
                date = reaction.get('date')
                detail_ref = self.parser.extract_reference(reaction.get('detail')).get('reference')
                reported = reaction.get('reported')
                
                reaction_parts = []
                if date:
                    reaction_parts.append(f"Date: {date}")
                if detail_ref:
                    reaction_parts.append(f"Detail: {detail_ref}")
                if reported is not None:
                    reaction_parts.append(f"Reported: {reported}")
                
                if reaction_parts:
                    reactions.append('; '.join(reaction_parts))
        
        # Extract protocol applied
        protocols = []
        if resource.get('protocolApplied'):
            for protocol in resource['protocolApplied']:
                series = protocol.get('series')
                authority_ref = self.parser.extract_reference(protocol.get('authority')).get('reference')
                target_disease = self.parser.extract_codeable_concept(protocol.get('targetDisease', [{}])[0] if protocol.get('targetDisease') else None)
                dose_number = protocol.get('doseNumberPositiveInt') or protocol.get('doseNumberString')
                series_doses = protocol.get('seriesDosesPositiveInt') or protocol.get('seriesDosesString')
                
                protocol_parts = []
                if series:
                    protocol_parts.append(f"Series: {series}")
                if authority_ref:
                    protocol_parts.append(f"Authority: {authority_ref}")
                if target_disease:
                    protocol_parts.append(f"Target: {target_disease}")
                if dose_number:
                    protocol_parts.append(f"Dose: {dose_number}")
                if series_doses:
                    protocol_parts.append(f"Total: {series_doses}")
                
                if protocol_parts:
                    protocols.append('; '.join(protocol_parts))
        
        return {
            'id': resource.get('id'),
            'date': resource.get('date'),
            'resourceType': resource.get('resourceType'),
            'status': resource.get('status'),
            'status_reason': self.parser.extract_codeable_concept(resource.get('statusReason')),
            'vaccine_code': self.parser.extract_codeable_concept(resource.get('vaccineCode')),
            'vaccine_code_system': self.parser.extract_coding_details(resource.get('vaccineCode')).get('system'),
            'vaccine_code_code': self.parser.extract_coding_details(resource.get('vaccineCode')).get('code'),
            'patient_reference': self.parser.extract_reference(resource.get('patient')).get('reference'),
            'encounter_reference': self.parser.extract_reference(resource.get('encounter')).get('reference'),
            'occurrence_datetime': resource.get('occurrenceDateTime'),
            'occurrence_string': resource.get('occurrenceString'),
            'recorded': resource.get('recorded'),
            'primary_source': resource.get('primarySource'),
            'report_origin': self.parser.extract_codeable_concept(resource.get('reportOrigin')),
            'location_reference': self.parser.extract_reference(resource.get('location')).get('reference'),
            'manufacturer_reference': self.parser.extract_reference(resource.get('manufacturer')).get('reference'),
            'lot_number': resource.get('lotNumber'),
            'expiration_date': resource.get('expirationDate'),
            'site': self.parser.extract_codeable_concept(resource.get('site')),
            'route': self.parser.extract_codeable_concept(resource.get('route')),
            'dose_quantity_value': self.parser.extract_quantity(resource.get('doseQuantity')).get('value'),
            'dose_quantity_unit': self.parser.extract_quantity(resource.get('doseQuantity')).get('unit'),
            'performers': ' | '.join(performers) if performers else None,
            'location_reference': self.parser.extract_reference(resource.get('location')).get('reference'),
            'reason_code': self.parser.extract_codeable_concept(resource.get('reasonCode', [{}])[0] if resource.get('reasonCode') else None),
            'reason_reference': self.parser.extract_reference(resource.get('reasonReference', [{}])[0] if resource.get('reasonReference') else None).get('reference'),
            'body_site': self.parser.extract_codeable_concept(resource.get('bodySite', [{}])[0] if resource.get('bodySite') else None),
            'outcome': self.parser.extract_codeable_concept(resource.get('outcome')),
            'complication': self.parser.extract_codeable_concept(resource.get('complication', [{}])[0] if resource.get('complication') else None),
            'follow_up': self.parser.extract_codeable_concept(resource.get('followUp', [{}])[0] if resource.get('followUp') else None),
            'note': resource.get('note', [{}])[0].get('text') if resource.get('note') else None,
            '_source_file': resource.get('_source_file'),
            '_processed_at': resource.get('_processed_at')
        }
            
    def _process_patient_info(self, patient_info: Dict) -> tuple[str, Dict]:
        """
        Process patient information from a FHIR Bundle.

        Args:
            patient_info (Dict): Patient information from a FHIR Bundle.

        Returns:
            tuple[str, Dict]: A tuple containing a string of patient information and a dictionary of processed patient data. 
                            Patient description should be like: Patient has Hypertension, Diabetes, Chronic sinusitis. Allergic to Shellfish and grass pollen. Age 74, Boston, MA.
        """
        info = ""
        out = {}
        
        # --- Patient ---
        patient_res = patient_info.get("Patient")
        if patient_res:
            patient_res = patient_res[0]
            # calculate current age
            today = date.today()
            birth_date = datetime.strptime(patient_res.get("birth_date"), "%Y-%m-%d").date()
            age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
            
            patient_dict = {
                "resource_type": "Patient",
                "full_name": patient_res.get("full_name"),
                "gender": patient_res.get("gender"),
                "birth_date": patient_res.get("birth_date"),
                "phone": patient_res.get("phone"),
                "email": patient_res.get("email"),
                "address": patient_res.get("address_full"),
                "country": patient_res.get("address_country"),
                "state" : patient_res.get("address_state"),
                "city" : patient_res.get("address_city"),
                "current_age": age,
                'ssn_id': patient_res.get("ssn"),
                'mrn_id': patient_res.get("mrn"),
                'uuid_id': patient_res.get("uuid"),
                'driver_license': patient_res.get("driver_license"),
                'passport_number': patient_res.get("passport"),
                'deceased_datetime': self.iso_to_sql(patient_res.get("deceased_datetime")),
            }
            out["Patient"] = patient_dict
            
            # String info
            # if patient_dict.get("full_name"):
            #     info += patient_dict["full_name"]
            # if patient_dict.get("gender"):
            #     info += f" - Gender: ({patient_dict['gender']})"
            # if patient_dict.get("birth_date"):
            #     info += f" - DOB: {patient_dict['birth_date']}"
            # if patient_dict.get("phone"):
            #     info += f" Phone: {patient_dict['phone']}"
            # if patient_dict.get("email"):
            #     info += f" Email: {patient_dict['email']}"
            if patient_dict.get("address"):
                address_str = f" Address: {patient_dict['address']}"
            else:
                address_str = ""
            if patient_dict.get("current_age"):
                age_str = f" Age: {patient_dict['current_age']}"
            else:
                age_str = ""
                
        # --- Allergies ---
        allergies = patient_info.get("AllergyIntolerance")
        if allergies:
            allergy_str = "Allergic to: "
            for allergy in allergies:
                allergy["date"] = allergy.get("date")
                al_list = []
                al_list.append({
                    "resource_type": "AllergyIntolerance",
                    "type": allergy.get("type"),
                    "category": allergy.get("category"),
                    "criticality": allergy.get("criticality"),
                    "code": allergy.get("code"),
                    "verification_status": allergy.get("verificationStatus"),
                    "clinical_status": allergy.get("clinicalStatus"),
                    "assertedDate": self.iso_to_sql(allergy.get("assertedDate"))
                })
                allergy_name = allergy.get("code").replace('Allergy to ', ' ').replace('allergy', ' ')
                if allergy_str != "Allergic to: ":
                    allergy_str += ", "
                allergy_str += f" {allergy_name} (severity: {allergy.get("criticality")})"
            out["AllergyIntolerance"] = {"resource_type": "AllergyIntolerance", "elements": al_list} 
        else:
            allergy_str = ""
            
        # --- Immunizations ---
        immunizations = patient_info.get("Immunization")
        if immunizations:
            im_list = []
            for imm in immunizations:
                im_list.append({
                    "resource_type": "Immunization",
                    "vaccine_code": imm.get("vaccine_code"),
                    #"vaccine_code_system": imm.get("vaccine_code_system"),
                    "imm_date": self.iso_to_sql(imm.get("date")),
                    "status": imm.get("status")
                })
                #info += f" - Immunization: {imm.get("vaccineCode")} date: {imm.get("date")}"
            out["Immunization"] = {"resource_type": "Immunization", "elements": im_list}
            
        # --- Observations ---
        observations = patient_info.get("Observation")
        if observations:
            obs_list = []
            for obs in observations:
                obs_list.append({
                    "resource_type": "Observation",
                    "code": obs.get("code"),
                    "obs_date": self.iso_to_sql(obs.get("effective_datetime")),
                    "value": obs.get("value_quantity"),
                    "unit": obs.get("value_unit")
                })
            #     info += f" - Observation: {obs.get("code")} date: {obs.get("date")} value: {obs.get("value_quantity")} {obs.get("value_unit")}"
            out["Observation"] = {"resource_type": "Observation", "elements": obs_list}
            
        # --- Conditions ---
        conditions = patient_info.get("Condition")
        if conditions:
            cnd_list = []
            conditions_str = "Patient has: "
            for cond in conditions:
                cnd_list.append({
                    "resource_type": "Condition",
                    "code": cond.get("code"),
                    "onset": self.iso_to_sql(cond.get("onset_datetime")),
                    "verification_status": cond.get("verificationStatus"),
                    "clinical_status": cond.get("clinicalStatus"),
                    "assertedDate": self.iso_to_sql(cond.get("assertedDate"))
                })
                if conditions_str != "Patient has: ":
                    conditions_str += ", "
                conditions_str += f" {cond.get("code")}"
                # info += f" - Condition: {cond.get("code")} date: {cond.get("onset_datetime")}"
            out["Condition"] = {"resource_type": "Condition", "elements": cnd_list}
        else:
            conditions_str = ""
            
        # --- Encounters ---
        # encounters = patient_info.get("Encounter")
        # if encounters:
        #     enc_list = []
        #     for enc in encounters:
        #         enc_list.append({
        #             "resource_type": "Encounter",
        #             "type": enc.get("type"),
        #             "start": enc.get("period_start"),
        #             "end": enc.get("period_end")
        #         })
        #         #info += f" - Encounter: {enc.get("type")} date from: {enc.get("period_start")} date to: {enc.get("period_end")}"
        #     out["Encounter"] = {"resource_type": "Encounter", "elements": enc_list}
            
        # --- Procedures ---
        procedures = patient_info.get("Procedure")
        if procedures:
            prc_list = []
            for proc in procedures:
                prc_list.append({
                    "resource_type": "Procedure",
                    "code": proc.get("code"),
                    "proc_date": self.iso_to_sql(proc.get("performed_period_start"))
                })
                #info += f" - Procedure: {proc.get("code")} date: {proc.get("performed_period_start")}"
            out["Procedures"] = {"resource_type": "Procedures", "elements": prc_list}

        # --- Diagnostic Reports ---
        # diagnostic_reports = patient_info.get("DiagnosticReport")
        # if diagnostic_reports:
        #     dr_list = []
        #     for dr in diagnostic_reports:
        #         dr_list.append({
        #             "resource_type": "DiagnosticReport",
        #             "code": dr.get("code"),
        #             "issued": dr.get("issued")
        #         })
        #         #info += f" - Diagnostic Report: {dr.get("code")} date: {dr.get("issued")}"
        #     out["DiagnosticReport"] = {"resource_type": "DiagnosticReport", "elements": dr_list}

        # --- Care Plans ---
        care_plans = patient_info.get("CarePlan")
        if care_plans:
            cp_list = []
            for cp in care_plans:
                cp_list.append({
                    "resource_type": "CarePlan",
                    "category": cp.get("category"),
                    "cp_start": self.iso_to_sql(cp.get("period_start")),
                    "CP_end": self.iso_to_sql(cp.get("period_end")),
                    "status": cp.get("status"),
                    "activities": cp.get("activities")
                })
                #info += f" - Care Plan: {cp.get("category")} date from: {cp.get("period_start")} date to: {cp.get("period_end")} status: {cp.get('status')} activities: {cp.get('activities')}"
            out["CarePlan"] = {"resource_type": "CarePlan", "elements": cp_list}
        
        info = f"{conditions_str}. {allergy_str}. {age_str}. {address_str}"
        
        return info, out

    def extract_patient_identifiers(self,bundle:dict) -> str:
        # get entry
        for resource in bundle['entry']:
            if resource['resource']['resourceType'] == 'Patient':
                patient_data = self._extract_patient_data(resource['resource'])
                return patient_data['uuid_id']
            
    @staticmethod
    def iso_to_sql(iso_ts: str | None) -> str:
        """
        Convert ISO-8601 timestamp into SQL TIMESTAMP string: 'YYYY-MM-DD HH:MM:SS'.
        """
        try:
            if iso_ts is None:
                return None
            # Parse ISO-8601 (supports timezone offsets)
            dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
            # Format into ODBC timestamp (drops timezone, uses UTC/local dt)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            raise ValueError(f"Invalid ISO timestamp: {iso_ts}") from e
