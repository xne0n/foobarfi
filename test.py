import streamlit as st
import json
import functools
import os
import tempfile
from typing import Any, Dict
from streamlit.web.server.oauth_authlib_routes import AuthCallbackHandler, create_oauth_client

# Use file-based storage that persists across processes
STORAGE_DIR = tempfile.gettempdir()
STORAGE_FILE = os.path.join(STORAGE_DIR, "streamlit_token_storage.json")

def load_token_storage() -> Dict[str, Any]:
    """Load token storage from file"""
    try:
        if os.path.exists(STORAGE_FILE):
            with open(STORAGE_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        print(f"DEBUG: Error loading token storage: {e}")
    return {}

def save_token_storage(storage: Dict[str, Any]) -> None:
    """Save token storage to file"""
    try:
        with open(STORAGE_FILE, 'w') as f:
            json.dump(storage, f)
        print(f"DEBUG: Saved token storage to {STORAGE_FILE}")
    except Exception as e:
        print(f"DEBUG: Error saving token storage: {e}")

def enhanced_auth_callback_get(original_method):
    """Monkey patch for AuthCallbackHandler.get to capture full token information"""
    @functools.wraps(original_method)
    async def wrapper(self):
        provider = self._get_provider_by_state()
        origin = self._get_origin_from_secrets()
        
        if origin is None or provider is None:
            return await original_method(self)
        
        error = self.get_argument("error", None)
        if error:
            return await original_method(self)

        client, _ = create_oauth_client(provider)
        
        # Get the full token object (this is the key part!)
        token = client.authorize_access_token(self)
        print("DEBUG: Token object:", token)
        user = token.get("userinfo")
        print("DEBUG: User object:", user)

        if user:
            # Store the full token information in persistent storage
            user_id = user.get('sub') or user.get('email') or user.get('oid')
            print(f"DEBUG: Extracted user_id: {user_id}")
            
            # Load existing storage
            token_storage = load_token_storage()
            print(f"DEBUG: token_storage before assignment: {token_storage}")
            
            if user_id:
                token_storage[user_id] = {
                    'full_token': token,
                    'access_token': token.get('access_token'),
                    'id_token': token.get('id_token'),
                    'refresh_token': token.get('refresh_token'),
                    'token_type': token.get('token_type'),
                    'expires_in': token.get('expires_in'),
                    'scope': token.get('scope'),
                    'userinfo': user
                }
                
                # Save to persistent storage
                save_token_storage(token_storage)
                print(f"DEBUG: Successfully stored token for user_id: {user_id}")
            else:
                print("DEBUG: user_id is None or empty!")
            
            # Continue with original Streamlit flow
            cookie_value = dict(user, origin=origin, is_logged_in=True)
            self.set_auth_cookie(cookie_value)
        else:
            print("DEBUG: user is None or empty!")
        
        self.redirect_to_base()
    
    return wrapper

# Apply the monkey patch
original_get = AuthCallbackHandler.get
AuthCallbackHandler.get = enhanced_auth_callback_get(original_get)

def get_full_token_info() -> Dict[str, Any]:
    """
    Get the full token information for the current user.
    This reuses Streamlit's existing user info to identify the user.
    """
    token_storage = load_token_storage()
    print(f"DEBUG: get_full_token_info called, token_storage: {token_storage}")
    
    if not st.user.is_logged_in:
        print("DEBUG: User not logged in")
        return {}
    
    # Use Streamlit's existing user identification
    user_id = st.user.get('sub') or st.user.get('email') or st.user.get('oid')
    print(f"DEBUG: Looking for user_id: {user_id}")
    print(f"DEBUG: Available keys in token_storage: {list(token_storage.keys())}")
    
    if user_id and user_id in token_storage:
        print(f"DEBUG: Found token for user_id: {user_id}")
        return token_storage[user_id]
    
    print(f"DEBUG: No token found for user_id: {user_id}")
    return {}

def get_access_token() -> str:
    """Get the access token for the current user"""
    token_info = get_full_token_info()
    return token_info.get('access_token', '')

def get_id_token() -> str:
    """Get the ID token for the current user"""
    token_info = get_full_token_info()
    return token_info.get('id_token', '')

def make_authenticated_api_call(api_url: str, headers: Dict[str, str] = None) -> Dict[str, Any]:
    """
    Make an API call using the stored access token.
    This demonstrates how to use the captured token for API calls.
    """
    import requests
    
    access_token = get_access_token()
    if not access_token:
        return {"error": "No access token available"}
    
    call_headers = headers or {}
    call_headers['Authorization'] = f'Bearer {access_token}'
    
    try:
        response = requests.get(api_url, headers=call_headers)
        return response.json()
    except Exception as e:
        return {"error": str(e)}

def clear_token_storage_for_user(user_id: str) -> None:
    """Clear token storage for a specific user"""
    try:
        token_storage = load_token_storage()
        if user_id in token_storage:
            del token_storage[user_id]
            save_token_storage(token_storage)
            print(f"DEBUG: Cleared token storage for user_id: {user_id}")
    except Exception as e:
        print(f"DEBUG: Error clearing token storage: {e}")

# Streamlit UI
st.title("Enhanced Streamlit Authentication with Full Token Access")

if not st.user.is_logged_in:
    st.write("Please log in to see your full token information.")
    if st.button("Log in"):
        st.login()
else:
    st.success(f"Welcome back, {st.user.get('name', 'User')}!")
    
    # Display standard Streamlit user info
    st.subheader("Standard Streamlit User Info")
    st.json(dict(st.user))
    
    # Display enhanced token information
    st.subheader("Enhanced Token Information")
    token_info = get_full_token_info()
    
    if token_info:
        st.write("**Access Token:**")
        access_token = get_access_token()
        if access_token:
            # Only show first and last few characters for security
            masked_token = f"{access_token[:10]}...{access_token[-10:]}" if len(access_token) > 20 else access_token
            st.code(masked_token)
        
        st.write("**ID Token:**")
        id_token = get_id_token()
        if id_token:
            masked_id_token = f"{id_token[:10]}...{id_token[-10:]}" if len(id_token) > 20 else id_token
            st.code(masked_id_token)
        
        st.write("**Full Token Structure:**")
        # Create a safe version without actual token values for display
        safe_token_info = {
            'access_token': '***MASKED***' if token_info.get('access_token') else None,
            'id_token': '***MASKED***' if token_info.get('id_token') else None,
            'refresh_token': '***MASKED***' if token_info.get('refresh_token') else None,
            'token_type': token_info.get('token_type'),
            'expires_in': token_info.get('expires_in'),
            'scope': token_info.get('scope'),
        }
        st.json(safe_token_info)
        
        # Example API call section
        st.subheader("Example: Make Authenticated API Call")
        st.write("You can now use the access token to make authenticated API calls:")
        
        # Example for Google APIs if using Google OAuth
        if 'accounts.google.com' in st.user.get('iss', ''):
            if st.button("Get Google User Profile"):
                profile_data = make_authenticated_api_call(
                    "https://www.googleapis.com/oauth2/v2/userinfo"
                )
                st.json(profile_data)
        
        # Example for Microsoft Graph API if using Microsoft OAuth
        elif 'login.microsoftonline.com' in st.user.get('iss', ''):
            if st.button("Get Microsoft User Profile"):
                profile_data = make_authenticated_api_call(
                    "https://graph.microsoft.com/v1.0/me"
                )
                st.json(profile_data)
        
        # Custom API endpoint
        st.write("**Custom API Call:**")
        api_url = st.text_input("Enter API URL:", placeholder="https://api.example.com/user")
        if st.button("Make API Call") and api_url:
            api_data = make_authenticated_api_call(api_url)
            st.json(api_data)
    
    else:
        st.warning("Token information not available. Please log out and log in again.")
    
    if st.button("Log out"):
        # Clear our token storage when user logs out
        user_id = st.user.get('sub') or st.user.get('email') or st.user.get('oid')
        if user_id:
            clear_token_storage_for_user(user_id)
        st.logout()
