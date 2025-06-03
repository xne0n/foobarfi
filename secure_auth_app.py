import streamlit as st
import json
import secrets
import time
import hashlib
import hmac
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlencode, parse_qs
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta

import tornado.web
import tornado.ioloop
import requests
from authlib.integrations.base_client import OAuthError
from authlib.integrations.requests_client import OAuth2Session
from authlib.oauth2.rfc6749 import OAuth2Token
from authlib.oidc.core import CodeIDToken

# Secure in-memory session storage
class SecureSessionStore:
    """Secure in-memory session storage with automatic cleanup."""
    
    def __init__(self):
        self._sessions: Dict[str, Dict[str, Any]] = {}
        self._session_expiry: Dict[str, float] = {}
        self._cleanup_interval = 300  # 5 minutes
        self._last_cleanup = time.time()
    
    def _cleanup_expired_sessions(self):
        """Remove expired sessions."""
        now = time.time()
        if now - self._last_cleanup < self._cleanup_interval:
            return
            
        expired_sessions = [
            session_id for session_id, expiry_time in self._session_expiry.items()
            if now > expiry_time
        ]
        
        for session_id in expired_sessions:
            self._sessions.pop(session_id, None)
            self._session_expiry.pop(session_id, None)
        
        self._last_cleanup = now
    
    def create_session(self, user_id: str, session_data: Dict[str, Any], ttl_seconds: int = 3600) -> str:
        """Create a new session and return session ID."""
        self._cleanup_expired_sessions()
        
        # Generate a cryptographically secure session ID
        session_id = secrets.token_urlsafe(32)
        expiry_time = time.time() + ttl_seconds
        
        self._sessions[session_id] = {
            'user_id': user_id,
            'created_at': time.time(),
            'last_accessed': time.time(),
            **session_data
        }
        self._session_expiry[session_id] = expiry_time
        
        return session_id
    
    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get session data and update last access time."""
        self._cleanup_expired_sessions()
        
        if session_id in self._sessions and session_id in self._session_expiry:
            # Check if session is still valid
            if time.time() < self._session_expiry[session_id]:
                self._sessions[session_id]['last_accessed'] = time.time()
                return self._sessions[session_id].copy()
        
        return None
    
    def update_session(self, session_id: str, data: Dict[str, Any]) -> bool:
        """Update session data."""
        self._cleanup_expired_sessions()
        
        if session_id in self._sessions:
            self._sessions[session_id].update(data)
            self._sessions[session_id]['last_accessed'] = time.time()
            return True
        return False
    
    def delete_session(self, session_id: str) -> bool:
        """Delete a session."""
        deleted = session_id in self._sessions
        self._sessions.pop(session_id, None)
        self._session_expiry.pop(session_id, None)
        return deleted

@dataclass
class TokenInfo:
    """Secure token information container."""
    access_token: str
    token_type: str = "Bearer"
    expires_in: Optional[int] = None
    refresh_token: Optional[str] = None
    scope: Optional[str] = None
    id_token: Optional[str] = None
    issued_at: float = None
    
    def __post_init__(self):
        if self.issued_at is None:
            self.issued_at = time.time()
    
    @property
    def is_expired(self) -> bool:
        """Check if the access token is expired."""
        if self.expires_in is None:
            return False
        return (time.time() - self.issued_at) >= self.expires_in
    
    @property
    def expires_at(self) -> Optional[float]:
        """Get the expiration timestamp."""
        if self.expires_in is None:
            return None
        return self.issued_at + self.expires_in

class SecureOAuthManager:
    """Secure OAuth manager using Authlib."""
    
    def __init__(self, client_id: str, client_secret: str, server_metadata_url: str, 
                 redirect_uri: str, scopes: str = "openid profile email"):
        self.client_id = client_id
        self.client_secret = client_secret
        self.server_metadata_url = server_metadata_url
        self.redirect_uri = redirect_uri
        self.scopes = scopes
        
        # Fetch server metadata
        self._server_metadata = self._fetch_server_metadata()
        
        # Create OAuth2 session
        self.oauth = OAuth2Session(
            client_id=client_id,
            client_secret=client_secret,
            scope=scopes,
            redirect_uri=redirect_uri
        )
    
    def _fetch_server_metadata(self) -> Dict[str, Any]:
        """Fetch OAuth server metadata."""
        try:
            response = requests.get(self.server_metadata_url, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            raise ValueError(f"Failed to fetch server metadata: {e}")
    
    def get_authorization_url(self, state: str) -> Tuple[str, str]:
        """Generate authorization URL with state parameter."""
        authorization_endpoint = self._server_metadata.get('authorization_endpoint')
        if not authorization_endpoint:
            raise ValueError("No authorization endpoint in server metadata")
        
        # Generate nonce for OIDC security
        nonce = secrets.token_urlsafe(32)
        
        url, state = self.oauth.create_authorization_url(
            authorization_endpoint,
            state=state,
            nonce=nonce
        )
        
        return url, nonce
    
    def exchange_code_for_tokens(self, code: str, state: str = None) -> TokenInfo:
        """Exchange authorization code for tokens."""
        token_endpoint = self._server_metadata.get('token_endpoint')
        if not token_endpoint:
            raise ValueError("No token endpoint in server metadata")
        
        try:
            token = self.oauth.fetch_token(
                token_endpoint,
                code=code,
                client_secret=self.client_secret
            )
            
            return TokenInfo(
                access_token=token['access_token'],
                token_type=token.get('token_type', 'Bearer'),
                expires_in=token.get('expires_in'),
                refresh_token=token.get('refresh_token'),
                scope=token.get('scope'),
                id_token=token.get('id_token')
            )
        except OAuthError as e:
            raise ValueError(f"Failed to exchange code for tokens: {e}")
    
    def refresh_access_token(self, refresh_token: str) -> TokenInfo:
        """Refresh access token using refresh token."""
        token_endpoint = self._server_metadata.get('token_endpoint')
        if not token_endpoint:
            raise ValueError("No token endpoint in server metadata")
        
        try:
            token = self.oauth.refresh_token(
                token_endpoint,
                refresh_token=refresh_token,
                client_secret=self.client_secret
            )
            
            return TokenInfo(
                access_token=token['access_token'],
                token_type=token.get('token_type', 'Bearer'),
                expires_in=token.get('expires_in'),
                refresh_token=token.get('refresh_token', refresh_token),  # Keep old refresh token if new one not provided
                scope=token.get('scope'),
                id_token=token.get('id_token')
            )
        except OAuthError as e:
            raise ValueError(f"Failed to refresh token: {e}")
    
    def get_userinfo(self, access_token: str) -> Dict[str, Any]:
        """Fetch user information from userinfo endpoint."""
        userinfo_endpoint = self._server_metadata.get('userinfo_endpoint')
        if not userinfo_endpoint:
            return {}
        
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/json'
        }
        
        try:
            response = requests.get(userinfo_endpoint, headers=headers, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Warning: Failed to fetch userinfo: {e}")
            return {}
    
    def parse_id_token(self, id_token: str) -> Dict[str, Any]:
        """Parse and validate ID token (simplified - in production use proper JWT validation)."""
        try:
            # In production, properly validate the JWT signature
            import base64
            
            # Split JWT parts
            parts = id_token.split('.')
            if len(parts) != 3:
                raise ValueError("Invalid JWT format")
            
            # Decode payload (add padding if needed)
            payload = parts[1]
            payload += '=' * (4 - len(payload) % 4)
            decoded = base64.urlsafe_b64decode(payload)
            
            return json.loads(decoded)
        except Exception as e:
            print(f"Warning: Failed to parse ID token: {e}")
            return {}

# Global secure session store
session_store = SecureSessionStore()

class SecureAuthenticatedUser:
    """Secure user info proxy that fetches live data."""
    
    def __init__(self):
        self._cached_data = None
        self._cache_expiry = 0
        self._cache_duration = 300  # 5 minutes
    
    def _get_session_data(self) -> Optional[Dict[str, Any]]:
        """Get current session data from secure storage."""
        # Get session ID from Streamlit's session state
        session_id = getattr(st.session_state, '_secure_auth_session_id', None)
        if not session_id:
            return None
        
        return session_store.get_session(session_id)
    
    def _refresh_cache(self) -> Dict[str, Any]:
        """Refresh user data cache."""
        session_data = self._get_session_data()
        if not session_data:
            return {'is_logged_in': False}
        
        # Check if we have valid tokens
        token_data = session_data.get('tokens')
        oauth_manager = session_data.get('oauth_manager')
        
        if not token_data or not oauth_manager:
            return {'is_logged_in': False}
        
        token_info = TokenInfo(**token_data)
        
        # Refresh token if expired
        if token_info.is_expired and token_info.refresh_token:
            try:
                new_token_info = oauth_manager.refresh_access_token(token_info.refresh_token)
                session_store.update_session(
                    getattr(st.session_state, '_secure_auth_session_id'),
                    {'tokens': asdict(new_token_info)}
                )
                token_info = new_token_info
            except Exception as e:
                print(f"Failed to refresh token: {e}")
                return {'is_logged_in': False}
        
        # Get fresh userinfo
        try:
            userinfo = oauth_manager.get_userinfo(token_info.access_token)
            
            # Combine ID token claims with fresh userinfo
            id_token_claims = {}
            if token_info.id_token:
                id_token_claims = oauth_manager.parse_id_token(token_info.id_token)
            
            combined_data = {
                **id_token_claims,
                **userinfo,
                'is_logged_in': True,
                'token_expires_at': token_info.expires_at,
                'has_refresh_token': bool(token_info.refresh_token)
            }
            
            self._cached_data = combined_data
            self._cache_expiry = time.time() + self._cache_duration
            
            return combined_data
            
        except Exception as e:
            print(f"Failed to get user info: {e}")
            return {'is_logged_in': False}
    
    def get_data(self) -> Dict[str, Any]:
        """Get current user data with caching."""
        if time.time() > self._cache_expiry or self._cached_data is None:
            return self._refresh_cache()
        return self._cached_data
    
    def get_access_token(self) -> Optional[str]:
        """Get current access token."""
        session_data = self._get_session_data()
        if not session_data:
            return None
        
        token_data = session_data.get('tokens')
        if not token_data:
            return None
        
        token_info = TokenInfo(**token_data)
        
        # Auto-refresh if expired
        if token_info.is_expired and token_info.refresh_token:
            try:
                oauth_manager = session_data.get('oauth_manager')
                new_token_info = oauth_manager.refresh_access_token(token_info.refresh_token)
                session_store.update_session(
                    getattr(st.session_state, '_secure_auth_session_id'),
                    {'tokens': asdict(new_token_info)}
                )
                return new_token_info.access_token
            except Exception as e:
                print(f"Failed to refresh token: {e}")
                return None
        
        return token_info.access_token if not token_info.is_expired else None
    
    def make_authenticated_request(self, url: str, method: str = 'GET', **kwargs) -> Optional[requests.Response]:
        """Make an authenticated HTTP request."""
        access_token = self.get_access_token()
        if not access_token:
            return None
        
        headers = kwargs.get('headers', {})
        headers['Authorization'] = f'Bearer {access_token}'
        kwargs['headers'] = headers
        
        try:
            return requests.request(method, url, **kwargs)
        except Exception as e:
            print(f"Request failed: {e}")
            return None
    
    def logout(self):
        """Securely logout the user."""
        session_id = getattr(st.session_state, '_secure_auth_session_id', None)
        if session_id:
            session_store.delete_session(session_id)
            if hasattr(st.session_state, '_secure_auth_session_id'):
                del st.session_state._secure_auth_session_id
        
        self._cached_data = None
        self._cache_expiry = 0

# Create global user instance
secure_user = SecureAuthenticatedUser()

def init_oauth_flow(provider_config: Dict[str, str]) -> str:
    """Initialize OAuth flow and return authorization URL."""
    oauth_manager = SecureOAuthManager(
        client_id=provider_config['client_id'],
        client_secret=provider_config['client_secret'],
        server_metadata_url=provider_config['server_metadata_url'],
        redirect_uri=provider_config['redirect_uri'],
        scopes=provider_config.get('scopes', 'openid profile email')
    )
    
    # Generate secure state parameter
    state = secrets.token_urlsafe(32)
    
    # Store OAuth manager and state in session state for callback
    st.session_state._oauth_manager = oauth_manager
    st.session_state._oauth_state = state
    
    auth_url, nonce = oauth_manager.get_authorization_url(state)
    st.session_state._oauth_nonce = nonce
    
    return auth_url

def handle_oauth_callback(code: str, state: str) -> bool:
    """Handle OAuth callback and create session."""
    # Verify state parameter
    expected_state = getattr(st.session_state, '_oauth_state', None)
    if not expected_state or state != expected_state:
        st.error("Invalid state parameter. Please try logging in again.")
        return False
    
    oauth_manager = getattr(st.session_state, '_oauth_manager', None)
    if not oauth_manager:
        st.error("OAuth manager not found. Please try logging in again.")
        return False
    
    try:
        # Exchange code for tokens
        token_info = oauth_manager.exchange_code_for_tokens(code, state)
        
        # Get user information
        userinfo = oauth_manager.get_userinfo(token_info.access_token)
        id_token_claims = {}
        if token_info.id_token:
            id_token_claims = oauth_manager.parse_id_token(token_info.id_token)
        
        # Create user ID from available claims
        user_id = (
            userinfo.get('sub') or 
            id_token_claims.get('sub') or 
            userinfo.get('email') or 
            id_token_claims.get('email') or
            userinfo.get('oid')  # Microsoft
        )
        
        if not user_id:
            st.error("Could not identify user. Please contact support.")
            return False
        
        # Create secure session
        session_id = session_store.create_session(
            user_id=user_id,
            session_data={
                'tokens': asdict(token_info),
                'oauth_manager': oauth_manager,
                'userinfo': userinfo,
                'id_token_claims': id_token_claims
            },
            ttl_seconds=86400  # 24 hours
        )
        
        # Store session ID in Streamlit session state
        st.session_state._secure_auth_session_id = session_id
        
        # Clean up temporary OAuth data
        for key in ['_oauth_manager', '_oauth_state', '_oauth_nonce']:
            if hasattr(st.session_state, key):
                delattr(st.session_state, key)
        
        return True
        
    except Exception as e:
        st.error(f"Authentication failed: {e}")
        return False

# UI Components
def render_login_page():
    """Render the login page with provider options."""
    st.title("🔐 Secure OAuth Authentication")
    
    st.markdown("""
    This implementation provides secure OAuth authentication with:
    - 🔒 **Secure in-memory session storage** (no file-based storage)
    - 🔄 **Automatic token refresh** 
    - 🛡️ **Proper state parameter validation**
    - 🔑 **Direct access to access tokens** for API calls
    - 🚫 **No monkey-patching** of Streamlit internals
    """)
    
    # Check for OAuth callback
    query_params = st.query_params
    if 'code' in query_params and 'state' in query_params:
        with st.spinner("Processing authentication..."):
            if handle_oauth_callback(query_params['code'], query_params['state']):
                st.success("✅ Authentication successful!")
                st.rerun()
            else:
                # Clear query params on error
                st.query_params.clear()
                st.rerun()
        return
    
    # Show login options
    st.subheader("Login Options")
    
    # Get auth config from secrets
    try:
        auth_config = st.secrets.get('auth', {})
        if not auth_config:
            st.error("No authentication configuration found in secrets.toml")
            st.info("Please configure your OAuth provider in `.streamlit/secrets.toml`")
            return
        
        # Get provider info from server metadata URL
        server_url = auth_config.get('server_metadata_url', '')
        provider_name = "Company SSO"
        provider_icon = "🏢"
        
        # Try to determine provider type from URL for better UX
        if 'google' in server_url.lower():
            provider_name = "Google"
            provider_icon = "🔍"
        elif 'microsoft' in server_url.lower():
            provider_name = "Microsoft"
            provider_icon = "🏢"
        elif 'okta' in server_url.lower():
            provider_name = "Okta"
            provider_icon = "🛡️"
        elif 'auth0' in server_url.lower():
            provider_name = "Auth0"
            provider_icon = "🔐"
        else:
            # Extract domain for custom providers
            try:
                from urllib.parse import urlparse
                domain = urlparse(server_url).netloc
                if domain:
                    provider_name = f"{domain.replace('auth.', '').replace('login.', '').split('.')[0].title()}"
            except:
                pass
        
        # Center the login button
        col1, col2, col3 = st.columns([1, 2, 1])
        
        with col2:
            if st.button(f"{provider_icon} Login with {provider_name}", use_container_width=True, type="primary"):
                try:
                    # Get custom scopes if specified
                    scopes = auth_config.get('scopes', 'openid profile email')
                    
                    auth_url = init_oauth_flow({
                        'client_id': auth_config['client_id'],
                        'client_secret': auth_config['client_secret'],
                        'server_metadata_url': auth_config['server_metadata_url'],
                        'redirect_uri': auth_config['redirect_uri'],
                        'scopes': scopes
                    })
                    st.markdown(f'<meta http-equiv="refresh" content="0; URL={auth_url}">', unsafe_allow_html=True)
                except Exception as e:
                    st.error(f"Failed to initialize OAuth: {e}")
                    st.error("Please check your OAuth configuration in secrets.toml")
        
        # Show configuration info
        with st.expander("🔧 Configuration Info"):
            st.write(f"**Provider:** {provider_name}")
            st.write(f"**Server:** {server_url}")
            st.write(f"**Client ID:** {auth_config.get('client_id', 'Not configured')}")
            st.write(f"**Redirect URI:** {auth_config.get('redirect_uri', 'Not configured')}")
            st.write(f"**Scopes:** {auth_config.get('scopes', 'openid profile email (default)')}")
    
    except Exception as e:
        st.error(f"Configuration error: {e}")
        st.error("Please check your `.streamlit/secrets.toml` file")

def render_user_dashboard():
    """Render the authenticated user dashboard."""
    user_data = secure_user.get_data()
    
    if not user_data.get('is_logged_in', False):
        render_login_page()
        return
    
    st.title(f"👋 Welcome, {user_data.get('name', user_data.get('email', 'User'))}!")
    
    # User info section
    with st.expander("👤 User Information", expanded=True):
        col1, col2 = st.columns(2)
        
        with col1:
            if 'picture' in user_data:
                st.image(user_data['picture'], width=100)
            
            st.write("**Email:**", user_data.get('email', 'N/A'))
            st.write("**Name:**", user_data.get('name', 'N/A'))
            
        with col2:
            if 'given_name' in user_data:
                st.write("**First Name:**", user_data['given_name'])
            if 'family_name' in user_data:
                st.write("**Last Name:**", user_data['family_name'])
            if 'locale' in user_data:
                st.write("**Locale:**", user_data['locale'])
    
    # Token info
    with st.expander("🔑 Token Information"):
        access_token = secure_user.get_access_token()
        if access_token:
            masked_token = f"{access_token[:10]}...{access_token[-10:]}"
            st.code(masked_token)
            
            if user_data.get('token_expires_at'):
                expires_at = datetime.fromtimestamp(user_data['token_expires_at'])
                st.write(f"**Expires at:** {expires_at}")
            
            st.write(f"**Has refresh token:** {user_data.get('has_refresh_token', False)}")
        else:
            st.warning("No valid access token available")
    
    # API testing section
    st.subheader("🧪 API Testing")
    
    col1, col2 = st.columns(2)
    
    with col1:
        api_url = st.text_input("API Endpoint:", placeholder="https://api.example.com/user")
        
        if st.button("🚀 Make Authenticated Request") and api_url:
            with st.spinner("Making request..."):
                response = secure_user.make_authenticated_request(api_url)
                if response:
                    st.success(f"Status: {response.status_code}")
                    try:
                        st.json(response.json())
                    except:
                        st.text(response.text)
                else:
                    st.error("Request failed")
    
    with col2:
        # Provider-specific quick actions
        issuer = user_data.get('iss', '')
        
        # Show some quick action buttons based on common endpoints
        st.write("**Quick Actions:**")
        
        # Generic userinfo endpoint test
        if st.button("👤 Refresh User Info"):
            with st.spinner("Refreshing user information..."):
                # Force refresh by clearing cache
                session_id = getattr(st.session_state, '_secure_auth_session_id', None)
                if session_id:
                    session_data = session_store.get_session(session_id)
                    if session_data:
                        # Clear cached userinfo to force refresh
                        session_data.pop('cached_userinfo', None)
                        session_data.pop('cache_timestamp', None)
                        session_store.update_session(session_id, session_data)
                st.rerun()
        
        # Provider-specific actions if we can detect them
        if 'accounts.google.com' in issuer:
            if st.button("📧 Get Gmail Profile"):
                with st.spinner("Fetching Gmail profile..."):
                    response = secure_user.make_authenticated_request(
                        "https://www.googleapis.com/gmail/v1/users/me/profile"
                    )
                    if response and response.status_code == 200:
                        st.json(response.json())
                    else:
                        st.error("Failed to fetch Gmail profile")
        
        elif 'login.microsoftonline.com' in issuer:
            if st.button("📊 Get Microsoft Profile"):
                with st.spinner("Fetching Microsoft profile..."):
                    response = secure_user.make_authenticated_request(
                        "https://graph.microsoft.com/v1.0/me"
                    )
                    if response and response.status_code == 200:
                        st.json(response.json())
                    else:
                        st.error("Failed to fetch Microsoft profile")
        
        else:
            # For custom/company providers, show a generic test
            if st.button("🔍 Test Token Validity"):
                with st.spinner("Testing access token..."):
                    access_token = secure_user.get_access_token()
                    if access_token:
                        st.success("✅ Access token is valid and not expired")
                        st.info(f"Token expires at: {datetime.fromtimestamp(user_data.get('token_expires_at', 0))}")
                    else:
                        st.error("❌ No valid access token available")
    
    # Raw user data
    with st.expander("🔍 Raw User Data"):
        st.json(user_data)
    
    # Logout
    st.divider()
    if st.button("🚪 Logout", type="primary"):
        secure_user.logout()
        st.rerun()

# Main app
def main():
    """Main application entry point."""
    st.set_page_config(
        page_title="Secure OAuth Demo",
        page_icon="🔐",
        layout="wide"
    )
    
    # Check if user is authenticated
    user_data = secure_user.get_data()
    
    if user_data.get('is_logged_in', False):
        render_user_dashboard()
    else:
        render_login_page()

if __name__ == "__main__":
    main() 