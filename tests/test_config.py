from khtb_lab.config import Settings
def test_defaults():
 s=Settings(); assert s.trading_env in {"PAPER","DEMO","LIVE"}; assert s.live_armed is False
 assert s.scan_seconds == 60
 assert s.position_manager_seconds == 15
