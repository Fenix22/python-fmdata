import unittest
from unittest.mock import Mock, patch

from fmdata.results import LoginResult
from fmdata.session_providers import (
    get_token_or_raise_exception,
    UsernamePasswordDataSource,
    UsernamePasswordLogin,
    ClarisCloudLogin
)


class TestSessionProviders(unittest.TestCase):

    def test_get_token_or_raise_exception_success(self):
        """Test get_token_or_raise_exception with successful login."""
        # Mock the LoginResult and response
        mock_response = Mock()
        mock_response.token = "test_token_123"

        mock_result = Mock(spec=LoginResult)
        mock_result.response = mock_response
        mock_result.raise_exception_if_has_error.return_value = None

        token = get_token_or_raise_exception(mock_result)

        self.assertEqual(token, "test_token_123")
        mock_result.raise_exception_if_has_error.assert_called_once()

    def test_get_token_or_raise_exception_with_error(self):
        """Test get_token_or_raise_exception when there's an error."""
        mock_result = Mock(spec=LoginResult)
        mock_result.raise_exception_if_has_error.side_effect = Exception("Login failed")

        with self.assertRaises(Exception) as context:
            get_token_or_raise_exception(mock_result)

        self.assertEqual(str(context.exception), "Login failed")
        mock_result.raise_exception_if_has_error.assert_called_once()


class TestUsernamePasswordDataSourceProvider(unittest.TestCase):

    def test_provide(self):
        """Test UsernamePasswordDataSourceProvider.provide method."""
        provider = UsernamePasswordDataSource(
            database="test_db",
            username="test_user",
            password="test_pass"
        )

        result = provider.provide()

        expected = {
            "database": "test_db",
            "username": "test_user",
            "password": "test_pass"
        }
        self.assertEqual(result, expected)


class TestUsernamePasswordSessionProvider(unittest.TestCase):

    def test_login_success(self):
        """Test UsernamePasswordSessionProvider.login method."""
        # Mock FMClient
        mock_client = Mock()
        mock_result = Mock(spec=LoginResult)
        mock_response = Mock()
        mock_response.token = "session_token_123"
        mock_result.response = mock_response
        mock_result.raise_exception_if_has_error.return_value = None
        mock_client.raw_login_username_password.return_value = mock_result

        provider = UsernamePasswordLogin(
            username="test_user",
            password="test_pass"
        )

        token = provider.login(mock_client)

        self.assertEqual(token, "session_token_123")
        mock_client.raw_login_username_password.assert_called_once_with(
            username="test_user",
            password="test_pass",
            data_sources=None
        )

    def test_login_with_data_sources(self):
        """Test UsernamePasswordSessionProvider.login with data sources."""
        mock_client = Mock()
        mock_result = Mock(spec=LoginResult)
        mock_response = Mock()
        mock_response.token = "session_token_123"
        mock_result.response = mock_response
        mock_result.raise_exception_if_has_error.return_value = None
        mock_client.raw_login_username_password.return_value = mock_result

        mock_data_source = Mock()
        provider = UsernamePasswordLogin(
            username="test_user",
            password="test_pass",
            data_sources=[mock_data_source]
        )

        token = provider.login(mock_client)

        self.assertEqual(token, "session_token_123")
        mock_client.raw_login_username_password.assert_called_once_with(
            username="test_user",
            password="test_pass",
            data_sources=[mock_data_source]
        )



class TestClarisCloudSessionProvider(unittest.TestCase):

    def test_init_defaults(self):
        """Test ClarisCloudSessionProvider initialization with defaults."""
        provider = ClarisCloudLogin()

        self.assertEqual(provider.cognito_userpool_id, 'us-west-2_NqkuZcXQY')
        self.assertEqual(provider.cognito_client_id, '4l9rvl4mv5es1eep1qe97cautn')
        self.assertIsNone(provider.claris_id_name)
        self.assertIsNone(provider.claris_id_password)
        self.assertIsNone(provider.data_sources)

    def test_init_custom_values(self):
        """Test ClarisCloudSessionProvider initialization with custom values."""
        provider = ClarisCloudLogin(
            cognito_userpool_id='custom_pool',
            cognito_client_id='custom_client',
            claris_id_name='test_user',
            claris_id_password='test_pass'
        )

        self.assertEqual(provider.cognito_userpool_id, 'custom_pool')
        self.assertEqual(provider.cognito_client_id, 'custom_client')
        self.assertEqual(provider.claris_id_name, 'test_user')
        self.assertEqual(provider.claris_id_password, 'test_pass')

    def test_get_cognito_token_missing_pycognito(self):
        """Test _get_cognito_token when pycognito is not available."""
        provider = ClarisCloudLogin(
            claris_id_name='test_user',
            claris_id_password='test_pass'
        )

        with patch('builtins.__import__', side_effect=ImportError):
            with self.assertRaises(ImportError) as context:
                provider._get_cognito_token()

            self.assertIn('Please install pycognito', str(context.exception))

    @patch('builtins.__import__')
    def test_get_cognito_token_success(self, mock_import):
        """Test _get_cognito_token successful authentication."""
        # Create mock pycognito module
        mock_pycognito = Mock()
        mock_user = Mock()
        mock_user.id_token = "cognito_token_123"
        mock_pycognito.Cognito.return_value = mock_user

        # Configure the import mock to return our mock pycognito
        mock_import.return_value = mock_pycognito

        provider = ClarisCloudLogin(
            cognito_userpool_id='test_pool',
            cognito_client_id='test_client',
            claris_id_name='test_user',
            claris_id_password='test_pass'
        )

        token = provider._get_cognito_token()

        self.assertEqual(token, "cognito_token_123")
        mock_pycognito.Cognito.assert_called_once_with(
            user_pool_id='test_pool',
            client_id='test_client',
            username='test_user'
        )
        mock_user.authenticate.assert_called_once_with('test_pass')

    @patch('builtins.__import__')
    def test_login_success(self, mock_import):
        """Test ClarisCloudSessionProvider.login method."""
        # Create mock pycognito module
        mock_pycognito = Mock()
        mock_user = Mock()
        mock_user.id_token = "cognito_token_123"
        mock_pycognito.Cognito.return_value = mock_user

        # Configure the import mock to return our mock pycognito
        mock_import.return_value = mock_pycognito

        # Mock FMClient
        mock_client = Mock()
        mock_result = Mock(spec=LoginResult)
        mock_response = Mock()
        mock_response.token = "claris_session_token"
        mock_result.response = mock_response
        mock_result.raise_exception_if_has_error.return_value = None
        mock_client.raw_login_claris_cloud.return_value = mock_result

        provider = ClarisCloudLogin(
            claris_id_name='test_user',
            claris_id_password='test_pass'
        )

        token = provider.login(mock_client)

        self.assertEqual(token, "claris_session_token")
        mock_client.raw_login_claris_cloud.assert_called_once_with(
            fmid_token="cognito_token_123",
            data_sources=None
        )


if __name__ == '__main__':
    unittest.main()
