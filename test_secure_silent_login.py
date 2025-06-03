#!/usr/bin/env python3
"""
Test and demonstration of the secure silent login functionality.

This script shows how to use the new secure implementation instead of
the insecure monkey-patching approach.
"""

import streamlit as st
from new_silent_login import (
    secure_silent_login_and_get_user_info,
    get_current_user_info,
    is_user_authenticated,
    get_current_access_token,
    logout_current_user
)

def main():
    """Main application demonstrating secure silent login."""
    st.set_page_config(
        page_title="Secure Silent Login Demo",
        page_icon="🔐",
        layout="wide"
    )
    
    st.title("🔐 Secure Silent Login Demo")
    st.markdown("This demo shows the secure replacement for `silent_login_full.py`")
    
    # App configuration
    app_name = "app_nfr_committees"
    
    # Show current authentication status
    with st.sidebar:
        st.header("Authentication Status")
        
        if is_user_authenticated():
            st.success("✅ User is authenticated")
            
            # Show user info without triggering login
            user_info = get_current_user_info(app_name)
            if user_info:
                st.write(f"**Email:** {user_info.get('email_address', 'N/A')}")
                st.write(f"**Name:** {user_info.get('first_name', '')} {user_info.get('last_name', '')}")
                st.write(f"**UID:** {user_info.get('uid', 'N/A')}")
                st.write(f"**Team:** {user_info.get('team_id', 'N/A')}")
                
                # Check access token
                access_token = get_current_access_token()
                if access_token:
                    st.write(f"**Token:** {access_token[:20]}...✅")
                else:
                    st.write("**Token:** ❌ No valid token")
            
            # Logout button
            if st.button("🚪 Logout", use_container_width=True):
                logout_current_user()
                st.rerun()
        else:
            st.warning("❌ User not authenticated")
            st.info("Click 'Secure Login' to authenticate")
    
    # Main content area
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🔒 Secure Silent Login")
        st.markdown("""
        This function will:
        1. Check if user is already authenticated
        2. Get user info from database if exists
        3. Create user in database if new
        4. Trigger secure OAuth flow if needed
        """)
        
        if st.button("🚀 Secure Login (Silent)", use_container_width=True, type="primary"):
            with st.spinner("Processing secure authentication..."):
                try:
                    # This is the main function - replaces silent_login_and_get_user_info
                    user_info = secure_silent_login_and_get_user_info(app_name)
                    
                    if user_info:
                        st.success("✅ Authentication successful!")
                        st.json(user_info)
                        
                        # Show what permissions user has
                        app_access = user_info.get(app_name, "None")
                        if app_access != "None":
                            st.success(f"✅ User has access to {app_name}: {app_access}")
                        else:
                            st.warning(f"⚠️ User has no access to {app_name}")
                    else:
                        st.info("🔄 Authentication in progress or failed")
                        
                except Exception as e:
                    st.error(f"❌ Error during authentication: {e}")
    
    with col2:
        st.subheader("📊 Current User Info")
        st.markdown("""
        This function checks current authentication state
        without triggering login flows.
        """)
        
        if st.button("📋 Get Current User Info", use_container_width=True):
            with st.spinner("Fetching user information..."):
                try:
                    # This gets user info without triggering login
                    user_info = get_current_user_info(app_name)
                    
                    if user_info:
                        st.success("✅ User found in database!")
                        
                        # Display user information
                        user_display = {
                            "Email": user_info.get('email_address', 'N/A'),
                            "First Name": user_info.get('first_name', 'N/A'),
                            "Last Name": user_info.get('last_name', 'N/A'),
                            "UID": user_info.get('uid', 'N/A'),
                            "Team ID": user_info.get('team_id', 'N/A'),
                            f"Access to {app_name}": user_info.get(app_name, 'None')
                        }
                        
                        for key, value in user_display.items():
                            st.write(f"**{key}:** {value}")
                    else:
                        if is_user_authenticated():
                            st.warning("⚠️ User authenticated but not in database")
                        else:
                            st.info("ℹ️ User not authenticated")
                            
                except Exception as e:
                    st.error(f"❌ Error fetching user info: {e}")
    
    # Token testing section
    st.subheader("🔑 Token Testing")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔍 Test Access Token", use_container_width=True):
            access_token = get_current_access_token()
            
            if access_token:
                st.success("✅ Valid access token available")
                st.code(f"Token: {access_token[:30]}...")
                
                # Test token with company API
                try:
                    from secure_auth_app import secure_user
                    
                    # You can replace this URL with your company's API endpoint
                    test_url = st.text_input(
                        "Test API URL:", 
                        placeholder="https://api.yourcompany.com/user"
                    )
                    
                    if test_url and st.button("🧪 Test API Call"):
                        response = secure_user.make_authenticated_request(test_url)
                        if response:
                            st.success(f"✅ API call successful (Status: {response.status_code})")
                            try:
                                st.json(response.json())
                            except:
                                st.text(response.text)
                        else:
                            st.error("❌ API call failed")
                            
                except Exception as e:
                    st.error(f"Error testing API: {e}")
            else:
                st.error("❌ No valid access token")
    
    with col2:
        st.subheader("📈 Authentication Statistics")
        
        # Show some stats about the current session
        if is_user_authenticated():
            from secure_auth_app import session_store
            
            # Count active sessions (secure - only visible within process)
            active_sessions = len(session_store._sessions)
            
            st.metric("Active Sessions", active_sessions)
            st.metric("Authentication Status", "✅ Authenticated")
            
            # Show token expiry if available
            user_data = get_current_user_info(app_name)
            if user_data:
                st.metric("User in Database", "✅ Yes")
            else:
                st.metric("User in Database", "❌ No")
        else:
            st.metric("Active Sessions", "0")
            st.metric("Authentication Status", "❌ Not Authenticated")
            st.metric("User in Database", "❓ Unknown")
    
    # Migration guide
    st.subheader("🔄 Migration from Old Method")
    
    with st.expander("See migration example"):
        st.markdown("""
        **Old insecure way:**
        ```python
        from silent_login_full import silent_login_and_get_user_info
        
        user_info = silent_login_and_get_user_info("app_nfr_committees")
        ```
        
        **New secure way:**
        ```python
        from new_silent_login import secure_silent_login_and_get_user_info
        
        user_info = secure_silent_login_and_get_user_info("app_nfr_committees")
        ```
        
        **Benefits:**
        - 🔒 Secure in-memory token storage
        - 🔄 Automatic token refresh
        - 🛡️ CSRF protection
        - 🧹 Automatic session cleanup
        - ⚡ Better performance
        - 🔧 Easier to maintain
        """)
    
    # Security info
    st.subheader("🛡️ Security Features")
    
    security_features = {
        "In-Memory Storage": "✅ Tokens stored securely in memory only",
        "Process Isolation": "✅ Sessions not accessible by other processes",
        "CSRF Protection": "✅ Cryptographically secure state parameters",
        "Auto Token Refresh": "✅ Tokens automatically refreshed before expiry",
        "Session Cleanup": "✅ Expired sessions automatically removed",
        "No Monkey-Patching": "✅ Uses Authlib directly, no internal modifications",
        "Thread Safety": "✅ Safe for concurrent users",
        "Audit Trail": "✅ Proper logging without exposing sensitive data"
    }
    
    for feature, description in security_features.items():
        st.write(f"**{feature}:** {description}")

if __name__ == "__main__":
    main() 