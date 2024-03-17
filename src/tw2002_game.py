from persistent_object import persistent_object

class tw2002clone_game(persistent_object):
    class persistent_data_spec:
        m_map, m_players, m_npcs, m_ships, m_planets, m_rulepack = None, None, None, None, None, None
        
    def __init__(self, map):
        self.m_map = map
        self.m_players = []
        self.m_npcs = []
        self.m_ships = []
        self.m_planets = []
        self.m_rulepack = []
        
    def add_player(self, p):
        pass
        
    def clock_update(self):
        pass