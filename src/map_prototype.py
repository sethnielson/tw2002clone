from persistent_object import persistent_object
from sqlite3_db import sqlite3_db

import random, time

class c_economy(persistent_object):
    class persistent_data_spec:
        standard_rates = None
        period = None
        
    def __init__(self):
        self.standard_rates = {
            'ore': 20,
            'fuel': 50,
            'organics': 10
        }
        self.period = 1*60
        

class c_starbase(persistent_object):
    class persistent_data_spec:
        holds = None
        gen_rate = None
        con_rate = None
        last_update = None
        economy = None
        last_update = None
        
    def __init__(self, economy):
        self.economy = economy
        self.holds = [None]*random.randint(10,100)
        sell_items = []
        buy_items = []
        rate_type = random.randint(1,3)
        if rate_type == 1:
            sell_items = random.sample(list(self.economy.standard_rates.keys()), k=random.randint(1,3))
        elif rate_type == 2:
            buy_items = random.sample(list(self.economy.standard_rates.keys()), k=random.randint(1,3))
        else:
            buy_items = random.sample(list(self.economy.standard_rates.keys()), k=random.randint(1,2))
            remaining_items = list(self.economy.standard_rates.keys())
            for b_item in buy_items:
                remaining_items.remove(b_item)
            sell_items = random.sample(remaining_items, k=random.randint(1, len(remaining_items)))
        self.gen_rate = {item_i: (random.random()/2)+0.25 for item_i in sell_items}
        self.con_rate = {item_i: (random.random()/2)+0.25 for item_i in buy_items}
        self.last_update = time.time()
        
    def get_sale_price(self, item):
        #print("Calculate sale price", item)
        if item not in self.gen_rate: 
            return None
        item_count = self.holds.count(item)
        free_space = self.holds.count(None)
        if item_count == 0: return None
        sale_price = self.economy.standard_rates[item]
        #print("Standard rate", sale_price)
        # multiply by gen_rate+.5 (gen-rate is between .25.75 so price between 75% and 125%)
        sale_price = int(sale_price * (self.gen_rate[item]+0.5))
        #print("General demand", sale_price)
        # multiply final price by overall sell drive.
        sale_price = int(sale_price * (.75 + (len(self.holds)-free_space)/(len(self.holds)*2)))
        #print("Inventory price", sale_price, (.75 + (len(self.holds)-free_space)/(len(self.holds)*2)))
        return sale_price
        
    def get_buy_price(self, item):
        if item not in self.con_rate: return None
        item_count = self.holds.count(item)
        free_space = self.holds.count(None)
        if free_space == 0: return None
        buy_price = self.economy.standard_rates[item]
        # multiply by gen_rate+.5 (con-rate is between .25.75 so price between 75% and 125%)
        buy_price = int(buy_price * (self.con_rate[item]+0.5))
        # multiply final price by overall buy drive.
        buy_price = int(buy_price * (.75 + free_space/(len(self.holds)*2)))
        print("inventory. Holds {}/{} full".format((len(self.holds)-free_space), len(self.holds)))
        print("Adjustment:", (.75 + free_space/(len(self.holds)*2)))
        return buy_price
        
    def dock(self):
        cur_time = time.time()
        elapsed = cur_time - self.last_update
        periods = int(elapsed / self.economy.period)
        free_holds = self.holds.count(None)
        print("Dock updating {} periods".format(periods))
        for period in range(periods):
            # removed consumed items
            for buy_item in self.con_rate.keys():
                if buy_item not in self.holds: continue
                if random.random() < self.con_rate[buy_item]:
                    print("Consume")
                    self.holds.remove(buy_item)
                    self.holds.append(None)
                    
            # update generated items
            for sell_item in self.gen_rate.keys():
                if self.holds.count(None) == 0: break
                if random.random() < self.gen_rate[sell_item]:
                    next_empty = self.holds.index(None)
                    self.holds[next_empty] = sell_item
        if periods > 0: 
            self.last_update = time.time()
        sell_prices = {sell_item: self.get_sale_price(sell_item) for sell_item in self.gen_rate}
        buy_prices  = {buy_item:  self.get_buy_price(buy_item)   for buy_item  in self.con_rate}
        print("Welcome to dock.")
        print("time: {}".format(time.ctime(time.time())))
        print("last update: {})".format(time.ctime(self.last_update)))
        print("Elapsed: {}".format(elapsed))
        print(self.holds)
        if sell_prices:
            print("\tItems for sale:")
            for sell_item in sell_prices:
                print("\t\t{}\t\t{}".format(sell_item, sell_prices[sell_item]))
        if buy_prices:
            print("\tItems to buy (max {}):".format(self.holds.count(None)))
            for buy_item in buy_prices:
                print("\t\t{}\t\t{}".format(buy_item, buy_prices[buy_item]))
        choice = None
        while choice is None:
            choice = input("[b, s, x] >> ").strip().lower().split(" ")
            try:
                if choice[0][0] == "b":
                    item = choice[1]
                    count = int(choice[2])
                    # buying sell-items. Sell-items from dock perspective, buy from trader
                    if item not in sell_prices:
                        print("No such item {}".format(item))
                        choice = None
                        continue
                    if count > self.holds.count(item):
                        print("Cannot buy {} {}. Insufficient quantity".format(count, item))
                        choice = None
                        continue
                    for i in range(count):
                        self.holds.remove(item)
                    print("You purchased {} {} for {} credits".format(count, item, count*sell_prices[item]))
                elif choice[0][0] == 's':
                    item = choice[1]
                    count = int(choice[2])
                    # selling buy-items. buy-items from dock perspective, sell from trader
                    if item not in buy_prices:
                        print("No such item {}".format(item))
                        choice = None
                        continue
                    if count > self.holds.count(None):
                        print("Cannot sell {} {}. Insufficient free holds".format(count, item))
                        choice = None
                        continue
                    for i in range(count):
                        next_empty = self.holds.index(None)
                        self.holds[next_empty] = item
                    print("You sold {} {} for {} credits".format(count, item, count*buy_prices[item]))
                else:
                    break
            except Exception as e:
                print(e)
                choice = None

class c_sector(persistent_object):
    class persistent_data_spec:
        jump_lanes = None
        name = None
        starbase = None
        
    def __init__(self, name):
        self.name = name
        self.jump_lanes = []
        self.starbase = None
        
    def add_star_base(self, b):
        self.starbase = b
        
    def add_jump_lane(self, s, one_way=False):
        if s not in self.jump_lanes:
            self.jump_lanes.append(s)
        if not one_way and self not in s.jump_lanes:
            s.jump_lanes.append(self)

class c_map_prototype(persistent_object):
    class persistent_data_spec:
        sectors = None
        current_sector = None
        economy = None
        
    def __init__(self):
        self.sectors = {}
        self.current_sector = None
        self.economy = c_economy()
        
    def generate(self, n):
        
        disconnected_sectors = set(range(1,n+1))
        links = {}
        for i in range(1, n+1):
            print("Generating sector", i)
            self.sectors[i] = c_sector(i)
            links[i] = random.randint(1,10)
        for i in range(1, n+1):
            print("Connecting sector", i, "to map")
            connection_options = [conn_i for conn_i in range(1,n+1) if conn_i != i and len(self.sectors[conn_i].jump_lanes) < links[conn_i]]
            # try and connect about half of expected connections. Others filled in by reverse connections
            if len(connection_options) == 0:
                break
            connections = random.sample(connection_options, k=(int(min(links[i], len(connection_options))/2)+1)) 
            for conn_i in connections:
                self.sectors[i].add_jump_lane(self.sectors[conn_i])
                disconnected_sectors.discard(i)
                disconnected_sectors.discard(conn_i)
        print("After initial connections, {} disconnected sectors remaining".format(len(disconnected_sectors)))
        for sec_i in disconnected_sectors:
            connection_options = [conn_i for conn_i in range(1, n+1) if conn_i != sec_i and links[conn_i] > 5]
            self.sectors[sec_i].add_jump_lane(self.sectors[random.choice(connection_options)])
        self.current_sector = 1
        #persistent_object.c_db().get_cache_control().write_to_database()
        #persistent_object.c_db().get_cache_control().change_mode("write_immediate")
        self.sectors[1].add_star_base(c_starbase(self.economy))
        for sector in self.sectors[1].jump_lanes:
            sector.add_star_base(c_starbase(self.economy))
        print("generation complete")
        
    def goto_sector(self, sector_number):
        if self.sectors[sector_number] in self.sectors[self.current_sector].jump_lanes:
            self.current_sector = sector_number
            return True
        else:
            return False
            
    def get_current_sector(self):
        return (self.current_sector, self.sectors[self.current_sector])

if __name__=="__main__":
    import sys, os
    if sys.argv[1] == "generate":
        if os.path.exists("test_map.db"): os.unlink("test_map.db")
        persistent_object.c_set_db_interface(sqlite3_db("test_map.db"))
        persistent_object.c_db().get_cache_control().change_mode("write_on_exit")
        map = c_map_prototype()
        map.generate(100)
        print("Map generated")
    elif sys.argv[1] == "play":
        persistent_object.c_set_db_interface(sqlite3_db("test_map.db"))
        persistent_object.c_db().get_cache_control().change_mode("write_on_exit")
        persistent_object.c_reload_objects()
        map = persistent_object.c_get_object_by_id(0)
        
        while True:
            sec_num, sector = map.get_current_sector()
            cur_sector  = "Sector [{}] (Unexplored)\n".format(sec_num)
            if sector.starbase:
                cur_sector += "Starbase\n"
            cur_sector += "Jump Lanes: {}\n".format([str(s.name) for s in sector.jump_lanes])
            cur_sector += ">> "
            cmd = input(cur_sector).strip().lower().split(" ")
            if cmd[0] in ["quit", "exit"]:
                break
            elif cmd[0] in ["hyper", "jump", "sector"]:
                try:
                    goto_sector = int(cmd[1])
                except:
                    print("Sector must be a number.")
                    continue
                result = map.goto_sector(goto_sector)
                if not result:
                    print("Cannot go to sector {}".format(goto_sector))
                else:
                    print("Jumping to sector {}".format(goto_sector))
            elif cmd[0] == "dock":
                if sector.starbase:
                    print("===> docking <===")
                    sector.starbase.dock()
                else:
                    print("There is no starbase here!")
            elif cmd[0] in ["exits", "jumps", "lanes"]:
                lanes = [str(s.name) for s in sector.jump_lanes]
                lanes.sort()
                print("Jump-lane exits from this sector are:", ", ".join(lanes))
        print("EXIT")