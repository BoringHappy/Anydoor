from anydoor.utils import Vault

if __name__ == "__main__":
    sec_name = "<name>"
    sec_value = {}
    
    Vault().add(sec_name, sec_value)
    
    result = Vault().get(sec_name)
    assert result.__dict__ == sec_value
