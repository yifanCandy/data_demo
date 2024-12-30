
# init package setting
print("Initializing mypackage...")

# import module
from .db_service import Db_Service


# Controlling what is imported
__all__ = ['Db_Service']