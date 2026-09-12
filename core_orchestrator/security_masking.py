import re

def strip_survivor_pii_for_broadcast(raw_telemetry: dict) -> dict:
    """
    Compliance Guardrail: Redacts names and explicit serial tracks 
    to preserve operational security (OpSec) during regional multi-agency handoffs.
    """
    sanitized_profile = raw_telemetry.copy()
    
    # Mask structural vessel ownership titles or names if present
    if "vessel_name" in sanitized_profile:
        sanitized_profile["vessel_name"] = "REDACTED // SAR OVERWATCH"
        
    # Obfuscate the exact serial marker while retaining the structural signature prefix
    if "serial_identifier" in sanitized_profile:
        orig_id = str(sanitized_profile["serial_identifier"])
        if len(orig_id) > 3:
            sanitized_profile["serial_identifier"] = f"XXX-{orig_id[-3:]}"
            
    return sanitized_profile