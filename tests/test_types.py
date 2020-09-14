import unittest.mock

from async_rediscache import types


@unittest.mock.patch("async_rediscache.types.RedisSession")
class RedisObjectNamespaceTests(unittest.IsolatedAsyncioTestCase):
    """Tests for the base RedisObject class."""

    def test_explicit_namespace_without_global_namespace(self, mock_session):
        """Test explicitly set namespace without a global namespace."""
        local_namespace = "van Rossum"

        mock_session.get_current_session().global_namespace = ""
        redis_object = types.RedisObject(namespace=local_namespace)
        self.assertEqual(redis_object.namespace, local_namespace)

    def test_explicit_namespace_with_global_namespace(self, mock_session):
        """Test explicitly set namespace with a global namespace."""
        global_namespace = "Guido"
        local_namespace = "van Rossum"
        expected_namespace = f"{global_namespace}.{local_namespace}"

        mock_session.get_current_session().global_namespace = global_namespace
        redis_object = types.RedisObject(namespace=local_namespace)
        self.assertEqual(redis_object.namespace, expected_namespace)

    def test_descriptor_based_namespace_without_global_namespace(self, mock_session):
        """Test namespace set by __set_name__ without a global namespace."""
        class Guido:
            van_rossum = types.RedisObject()

        mock_session.get_current_session().global_namespace = ""
        self.assertEqual(Guido.van_rossum.namespace, "Guido.van_rossum")

    def test_descriptor_based_namespace_with_global_namespace(self, mock_session):
        """Test namespace set by __set_name__ with a global namespace."""
        class Guido:
            van_rossum = types.RedisObject()

        mock_session.get_current_session().global_namespace = "core_dev"
        self.assertEqual(Guido.van_rossum.namespace, "core_dev.Guido.van_rossum")

    def test_set_name_picks_first_attribute_for_namespace(self, mock_session):
        """Test that __set_name__ only sets the namespace for the first assignment."""
        class Kyle:
            stanley = types.RedisObject()
            broflovski = stanley

        mock_session.get_current_session().global_namespace = ""
        self.assertEqual(Kyle.stanley.namespace, "Kyle.stanley")

    def test_set_name_does_not_override_explicit_namespace(self, mock_session):
        """Test that __set_name__ only sets the namespace for the first assignment."""
        class Kyle:
            stanley = types.RedisObject(namespace="python")

        mock_session.get_current_session().global_namespace = ""
        self.assertEqual(Kyle.stanley.namespace, "python")

    async def test_get_pool_connection_raises_without_set_namespace(self, _mock_session):
        """Test if `get_pool_connection` raises exception if a namespace wasn't set."""
        cache = types.RedisObject()

        with self.assertRaises(types.NoNamespaceError):
            await cache._get_pool_connection()

    def test_bypassing_global_namespace(self, mock_session):
        """Test if a RedisObject allows you to bypass the global namespace."""
        cache = types.RedisObject(namespace="Amsterdam", use_global_namespace=False)

        mock_session.get_current_session().global_namespace = "New"
        self.assertEqual(cache.namespace, "Amsterdam")
