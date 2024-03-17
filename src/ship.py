from persistent_object import persistent_object

class ship(persistent_object):
    class persistent_data_spec:
        m_class = None
        m_fighters = None
        m_holds = None
        m_turns = None
        m_special = None
        m_sector = None
        m_hp = None
        
    def __init__(self, ship_class):
        self.m_class = ship_class
        self.m_hp = self.m_class.max_hit_points()
        self.m_fighters = 0
        self.m_holds = [None] * self.m_class.num_min_holds()
        self.m_turns = self.m_class.turns_per_day()
        self.m_special = [item.create() for item in self.m_class.special_features()]
        self.m_sector = None
        
    def get_ui_view(self):
        pass
        

    