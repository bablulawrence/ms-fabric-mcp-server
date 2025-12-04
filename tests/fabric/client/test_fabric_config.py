"""Tests for Fabric configuration."""

import pytest
import os


@pytest.mark.unit
class TestFabricConfig:
    """Test suite for FabricConfig class."""
    
    def test_config_from_environment_defaults(self, monkeypatch):
        """Test loading configuration with default values."""
        from ms_fabric_mcp_server.client.config import FabricConfig
        
        # Clear environment
        monkeypatch.delenv("FABRIC_BASE_URL", raising=False)
        
        config = FabricConfig.from_environment()
        
        assert config.BASE_URL == "https://api.fabric.microsoft.com/v1"
        assert config.API_CALL_TIMEOUT == 30
        assert config.MAX_RETRIES == 3
    
    def test_config_from_environment_custom(self, monkeypatch):
        """Test loading configuration from environment variables."""
        from ms_fabric_mcp_server.client.config import FabricConfig
        
        monkeypatch.setenv("FABRIC_BASE_URL", "https://custom.api.com")
        monkeypatch.setenv("FABRIC_API_CALL_TIMEOUT", "60")
        monkeypatch.setenv("FABRIC_MAX_RETRIES", "5")
        
        config = FabricConfig.from_environment()
        
        assert config.BASE_URL == "https://custom.api.com"
        assert config.API_CALL_TIMEOUT == 60
        assert config.MAX_RETRIES == 5
    
    def test_config_scopes(self):
        """Test that scopes are configured correctly."""
        from ms_fabric_mcp_server.client.config import FabricConfig
        
        config = FabricConfig.from_environment()
        
        assert "https://api.fabric.microsoft.com/.default" in config.SCOPES
    
    def test_config_livy_timeouts(self):
        """Test Livy-specific timeout configurations."""
        from ms_fabric_mcp_server.client.config import FabricConfig
        
        config = FabricConfig.from_environment()
        
        assert hasattr(config, 'LIVY_SESSION_WAIT_TIMEOUT')
        assert hasattr(config, 'LIVY_STATEMENT_WAIT_TIMEOUT')
        assert hasattr(config, 'LIVY_POLL_INTERVAL')
    
    def test_config_retry_settings(self):
        """Test retry configuration."""
        from ms_fabric_mcp_server.client.config import FabricConfig
        
        config = FabricConfig.from_environment()
        
        assert config.MAX_RETRIES >= 0
        assert config.RETRY_BACKOFF > 0


@pytest.mark.unit
class TestFabricConfigExports:
    """Test Fabric config exports."""
    
    def test_config_exported_from_fabric_module(self):
        """Test that FabricConfig is exported from fabric module."""
        from ms_fabric_mcp_server import FabricConfig
        
        assert FabricConfig is not None
