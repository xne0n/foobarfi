import requests
import time
import os
from typing import Any, Dict, Optional
import pandas as pd
from database import engine_postgres
from sqlalchemy import text
import streamlit as st

# Import Streamlit's internal components for proper redirects
from streamlit.proto.ForwardMsg_pb2 import ForwardMsg
from streamlit.runtime.scriptrunner_utils.script_run_context import get_script_run_ctx
from streamlit.auth_util import encode_provider_token
from streamlit import config
from streamlit.url_util import make_url_path

# Import the secure OAuth components
from secure_auth_app import (
    session_store, 
    SecureOAuthManager, 
    TokenInfo, 
    secure_user,
    init_oauth_flow,
    handle_oauth_callback
)

def user_email_exists(email: str) -> bool:
    """Check if user exists in database by email."""
    sql_query = 'SELECT 1 FROM tbl_users WHERE "email_address" = %s LIMIT 1'
    params = (email,)
    df = pd.read_sql_query(sql_query, engine_postgres, params=params)
    return not df.empty

def get_user_by_email(email: str, app_name: str) -> dict:
    """Get user information from database by email."""
    sql_query = f'''
        SELECT uid, last_name, first_name, team_id, {app_name}, email_address
        FROM tbl_users WHERE "email_address" = %s LIMIT 1
    '''
    params = (email,)
    df = pd.read_sql_query(sql_query, engine_postgres, params=params)
    if not df.empty:
        return df.iloc[0].to_dict()
    return {}

def insert_user(user_data: dict, app_name: str = "app_nfr_committees") -> None:
    """Insert new user into database."""
    sql_query = '''
        INSERT INTO tbl_users (
            uid, last_name, first_name, team_id, app_nfr_committees, email_address
        ) VALUES (:uid, :last_name, :first_name, :team_id, :app_nfr_committees, :email_address)
    '''
    params = {
        "uid": user_data.get("uid"),
        "last_name": user_data.get("last_name"),
        "first_name": user_data.get("first_name"),
        "team_id": user_data.get("team_id"),
        "app_nfr_committees": "None",
        "email_address": user_data.get("email_address"),
    }
    with engine_postgres.begin() as conn:
        conn.execute(text(sql_query), params)

def get_userinfo_from_token(access_token: str, server_metadata_url: str) -> Dict[str, Any]:
    """Get user information from OAuth provider using access token."""
    try:
        # Fetch server metadata to get userinfo endpoint
        metadata_response = requests.get(server_metadata_url, timeout=10)
        metadata_response.raise_for_status()
        metadata = metadata_response.json()
        
        userinfo_endpoint = metadata.get("userinfo_endpoint")
        if not userinfo_endpoint:
            print("No userinfo endpoint found in server metadata")
            return {}
        
        # Make request to userinfo endpoint
        headers = {
            'Authorization': f"Bearer {access_token}",
            'Accept': 'application/json'
        }
        
        user_info_response = requests.get(
            userinfo_endpoint,
            headers=headers,
            timeout=10
        )
        user_info_response.raise_for_status()
        
        return user_info_response.json()
        
    except Exception as e:
        print(f"Error fetching userinfo: {e}")
        return {}

def extract_user_data_from_userinfo(user_info: Dict[str, Any]) -> Dict[str, Any]:
    """Extract and normalize user data from OAuth userinfo response."""
    return {
        'uid': user_info.get('login_ad') or user_info.get('sub') or user_info.get('preferred_username'),
        'first_name': user_info.get('first_name') or user_info.get('given_name'),
        'last_name': user_info.get('last_name') or user_info.get('family_name'),
        'email_address': user_info.get('mail') or user_info.get('email'),
        'team_id': user_info.get('rc_local_sigle') or user_info.get('department'),
    }

def generate_streamlit_login_url(provider: str = "default") -> str:
    """Generate Streamlit's internal login URL (same as st.login() uses)."""
    AUTH_LOGIN_ENDPOINT = "/auth/login"
    provider_token = encode_provider_token(provider)
    base_path = config.get_option("server.baseUrlPath")
    login_path = make_url_path(base_path, AUTH_LOGIN_ENDPOINT)
    return f"{login_path}?provider={provider_token}"

def secure_redirect_to_auth(auth_url: str) -> None:
    """
    Attempt a robust client-side redirect, prioritizing JavaScript.
    This aims to be as browser-agnostic as possible.
    """
    # Primary Method: Pure JavaScript redirect
    # This is generally the most reliable for forcing a browser navigation.
    js_redirect_script = f'''
        <script>
            window.location.replace("{auth_url}");
        </script>
    '''
    st.markdown(js_redirect_script, unsafe_allow_html=True)

    # Display a message to the user while the redirect is happening.
    # This also acts as a visual cue if the script somehow fails.
    st.info("🔄 Redirecting to authentication... Please wait.")
    st.markdown(f"If you are not redirected automatically, [please click here to continue]({auth_url}).")

    # Ensure Streamlit stops processing.
    # This needs to happen *after* the markdown has been sent to the browser.
    # A small delay might help ensure the JS has a chance to execute in some environments.
    time.sleep(0.1)  # Small delay to help ensure JS execution
    st.stop()

def secure_silent_login_and_get_user_info(app_name: str) -> Dict[str, Any]:
    """
    Securely performs silent login and returns user info for the given app name.
    
    This function uses the secure OAuth implementation instead of monkey-patching.
    It follows the same logic as the original but with proper security measures.
    
    Args:
        app_name: The application name to check permissions for
        
    Returns:
        Dictionary containing user information from database, or empty dict if not authenticated
    """
    
    # Step 1: Check if user is already authenticated via secure session
    user_data = secure_user.get_data()
    
    if user_data.get('is_logged_in', False):
        print("User already authenticated via secure session")
        
        # Get email from authenticated user data
        email = user_data.get('email') or user_data.get('mail')
        
        if email and user_email_exists(email):
            print(f"User {email} found in database")
            return get_user_by_email(email, app_name)
        
        # If user not in database, try to get fresh userinfo and create user
        if email:
            try:
                access_token = secure_user.get_access_token()
                if access_token:
                    # Get server metadata URL from secrets
                    auth_config = st.secrets.get('auth', {})
                    server_metadata_url = auth_config.get('server_metadata_url')
                    
                    if server_metadata_url:
                        # Get fresh userinfo from OAuth provider
                        fresh_userinfo = get_userinfo_from_token(access_token, server_metadata_url)
                        
                        if fresh_userinfo:
                            # Extract and normalize user data
                            user_data_db = extract_user_data_from_userinfo(fresh_userinfo)
                            
                            # Insert new user into database
                            if user_data_db.get('email_address'):
                                print(f"Creating new user: {user_data_db.get('email_address')}")
                                insert_user(user_data_db, app_name)
                                return get_user_by_email(user_data_db['email_address'], app_name)
                        
            except Exception as e:
                print(f"Error processing authenticated user: {e}")
    
    # Step 2: Check if we have any active sessions in secure storage
    print("Checking for existing secure sessions...")
    
    # Get all active sessions (this is secure - only accessible within the process)
    # We'll iterate through sessions to find a valid one
    for session_id, session_data in session_store._sessions.items():
        try:
            # Check if session is still valid
            if time.time() < session_store._session_expiry.get(session_id, 0):
                token_data = session_data.get('tokens')
                oauth_manager = session_data.get('oauth_manager')
                
                if token_data and oauth_manager:
                    token_info = TokenInfo(**token_data)
                    
                    # Try to refresh token if expired
                    if token_info.is_expired and token_info.refresh_token:
                        try:
                            new_token_info = oauth_manager.refresh_access_token(token_info.refresh_token)
                            token_info = new_token_info
                            # Update session with new token
                            session_data['tokens'] = new_token_info.__dict__
                            session_store.update_session(session_id, session_data)
                            print("Successfully refreshed access token")
                        except Exception as e:
                            print(f"Failed to refresh token: {e}")
                            continue
                    
                    # If we have a valid access token, try to get user info
                    if not token_info.is_expired and token_info.access_token:
                        auth_config = st.secrets.get('auth', {})
                        server_metadata_url = auth_config.get('server_metadata_url')
                        
                        if server_metadata_url:
                            user_info = get_userinfo_from_token(token_info.access_token, server_metadata_url)
                            
                            if user_info:
                                user_data_db = extract_user_data_from_userinfo(user_info)
                                email = user_data_db.get('email_address')
                                
                                if email:
                                    # Check if user exists in database
                                    if user_email_exists(email):
                                        print(f"Found existing user {email} in database")
                                        return get_user_by_email(email, app_name)
                                    else:
                                        # Create new user
                                        print(f"Creating new user {email} in database")
                                        insert_user(user_data_db, app_name)
                                        return get_user_by_email(email, app_name)
        
        except Exception as e:
            print(f"Error processing session {session_id}: {e}")
            continue
    
    # Step 3: No valid authentication found, trigger secure login
    print("No valid authentication found, need to login")
    
    # Check if we're in the middle of an OAuth callback
    query_params = st.query_params
    if 'code' in query_params and 'state' in query_params:
        print("Processing OAuth callback...")
        if handle_oauth_callback(query_params['code'], query_params['state']):
            print("OAuth callback successful, retrying...")
            # Recursively call this function after successful login
            return secure_silent_login_and_get_user_info(app_name)
        else:
            print("OAuth callback failed")
            return {}
    
    # If not in callback, check if OAuth flow is already initiated
    if not hasattr(st.session_state, '_oauth_manager'):
        print("Initiating OAuth flow...")
        try:
            auth_config = st.secrets.get('auth', {})
            if auth_config:
                # Get custom scopes if specified
                scopes = auth_config.get('scopes', 'openid profile email')
                
                auth_url = init_oauth_flow({
                    'client_id': auth_config['client_id'],
                    'client_secret': auth_config['client_secret'],
                    'server_metadata_url': auth_config['server_metadata_url'],
                    'redirect_uri': auth_config['redirect_uri'],
                    'scopes': scopes
                })
                
                # Use the exact same redirect mechanism as st.login()
                secure_redirect_to_auth(auth_url=auth_url)
                
        except Exception as e:
            print(f"Error initiating OAuth flow: {e}")
    
    print("Authentication flow in progress...")
    return {}

def get_current_user_info(app_name: str) -> Optional[Dict[str, Any]]:
    """
    Get current authenticated user info from secure session.
    
    This is a simpler version that only checks current authentication state
    without triggering login flows.
    
    Args:
        app_name: The application name to check permissions for
        
    Returns:
        Dictionary containing user information from database, or None if not authenticated
    """
    user_data = secure_user.get_data()
    
    if not user_data.get('is_logged_in', False):
        return None
    
    email = user_data.get('email') or user_data.get('mail')
    
    if email and user_email_exists(email):
        return get_user_by_email(email, app_name)
    
    return None

def is_user_authenticated() -> bool:
    """
    Check if user is currently authenticated via secure session.
    
    Returns:
        True if user is authenticated, False otherwise
    """
    user_data = secure_user.get_data()
    return user_data.get('is_logged_in', False)

def get_current_access_token() -> Optional[str]:
    """
    Get current access token for authenticated user.
    
    Returns:
        Valid access token or None if not authenticated
    """
    return secure_user.get_access_token()

def logout_current_user() -> None:
    """
    Securely logout the current user and clear all session data.
    """
    secure_user.logout()

# Example usage function
def example_usage():
    """
    Example of how to use the secure silent login functionality.
    """
    app_name = "app_nfr_committees"
    
    # Try to get user info (will trigger login if needed)
    user_info = secure_silent_login_and_get_user_info(app_name)
    
    if user_info:
        print(f"User authenticated: {user_info.get('email_address')}")
        print(f"User UID: {user_info.get('uid')}")
        print(f"User Name: {user_info.get('first_name')} {user_info.get('last_name')}")
        
        # Check if user has access to the app
        app_access = user_info.get(app_name, "None")
        print(f"Access to {app_name}: {app_access}")
        
        # Get access token for API calls
        access_token = get_current_access_token()
        if access_token:
            print(f"Access token available: {access_token[:20]}...")
    else:
        print("User not authenticated or authentication in progress")

if __name__ == "__main__":
    example_usage() 