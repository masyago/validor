import os

SESSION_COOKIE_NAME = "validor_session_id"
SESSION_TTL_MINUTES = int(os.getenv("SESSION_TTL_MINUTES", "30"))
