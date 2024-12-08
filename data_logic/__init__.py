
# init package setting
print("Initializing mypackage...")

# import module
from .data_logic import Data_logic


# Controlling what is imported
__all__ = ['Data_logic']
