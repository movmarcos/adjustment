import os
import sys

# notebooks/ — so `import adjustment_test_kit as kit` works.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
# streamlit_app/ — so the parity tests can `import utils.scope_filters` and
# `import utils.snowflake_conn` exactly the way the app's own tests do.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "streamlit_app"))
