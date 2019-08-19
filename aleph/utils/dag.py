'''
    This is a Proof-of-Concept implementation of Aleph Zero consensus protocol.
    Copyright (C) 2019 Aleph Zero Team
    
    This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.
    This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
    
    You should have received a copy of the GNU General Public License
    along with this program. If not, see <http://www.gnu.org/licenses/>.
'''

class DAG:
    '''
    A class to represent the structural info about a poset (in the sense of Aleph) in (arguably) the simplest possible form.
    The main purpose of DAG is to have an alternative, maximally simple implementation of primitives used in the Poset class.
    In particular, it is often used in tests to run it against Poset.
    Every node in DAG is represented by a string (often referred to as its name), further:
        1) self.nodes is a dictionary indexed by names such that self.nodes[node] is the list of (names of) parents of node.
        2) self.pid[node] is the process_id of the creator of node
        3) some additional info is stored in self.levels, self.node_aux_info and self.prime_units_by_level etc.
    '''
    def __init__(self, n_processes, no_forkers = False):
        '''
        :param int n_processes: the number of processes that generate nodes in the dag/poset
        :param bool no_forkers: if set to true then it is guaranteed that there is no forking in the poset
                                 and that the first parent of every node is its self_predecessor
        '''
        self.n_processes = n_processes
        self.nodes = {}
        self.pids = {}
        self.levels = {}
        self.prime_units_by_level = {}
        # a dictionary of the type node -> dict(), where dict contains additional info for this particular node
        self.node_aux_info = {}
        self.no_forkers = no_forkers
        self.nodes_as_added = []


    def __contains__(self, node): return node in self.nodes
    def __iter__(self): return iter(self.nodes)
    def __len__(self): return len(self.nodes)

    def pid(self, node): return self.pids[node]
    def parents(self, node): return self.nodes[node]
    def level(self, node): return self.levels[node]
    def height(self, node): return self.get_node_info(node, "height")

    def get_node_list_as_added(self):
        '''
        Return a list of nodes in the dag in the same order as they have been added.
        Note that this in particular gives a topological ordering of nodes.
        '''
        return self.nodes_as_added

    def get_prime_units_by_level(self, level):
        if level not in self.prime_units_by_level:
            return []
        else:
            return self.prime_units_by_level[level]

    def update_prime_units(self, node):
        '''
        Given a freshly added node, updates the self.prime_units_by_level dict.
        '''
        level = self.level(node)
        if level not in self.prime_units_by_level:
            self.prime_units_by_level[level] = []
        if self.is_prime(node):
            self.prime_units_by_level[level].append(node)

    def is_prime(self, node):
        level = self.level(node)
        predecessor = self.self_predecessor(self.pids[node], parent_nodes = self.nodes[node])
        if predecessor is None or level > self.levels[predecessor]:
            self.prime_units_by_level[level].append(node)

    def add_node_info(self, node, key, value):
        '''
        Attaches some auxiliary info to some node in the dag.
        :param str node: the name of the node
        :param str key: the name of the info field we would like to add
        :param obj value: the value of the info field we would like to add
        '''
        if node not in self.node_aux_info:
            self.node_aux_info[node] = {}
        self.node_aux_info[node][key] = value

    def get_node_info(self, node, key):
        '''
        Reads the auxiliary info in a node under a given key.
        '''
        if node not in self.node_aux_info:
            return None
        return self.node_aux_info[node].get(key, None)

    def compute_node_height(self, node):
        '''
        Computes the node height, i.e. the length of the chain from start till the current node created by node's creator process.
        '''
        predecessor = self.self_predecessor(self.pids[node], parent_nodes = self.nodes[node])
        if predecessor is None:
            return 0
        else:
            return self.get_node_info(predecessor, "height") + 1

    def add(self, name, pid, parents, level_hint = None, aux_info = None):
        '''
        Adds a new node to the dag.
        :param str name: the name of the new node
        :param int pid: the creator_id of the new node
        :param list parents: the list of parents (names) of the node
        :param int level_hint: if not None, gives the level of the node, and thus saves on computation
        :param dict aux_info: a dictionary of auxiliary information that shall be attached to the new node
        '''
        assert all(p in self for p in parents), f"Parents of {name} are not in DAG"
        assert name not in self, "Node already in dag."

        self.pids[name] = pid
        self.nodes[name] = parents[:]

        if level_hint is None:
            # computing the level
            level = max([self.levels[p] for p in parents]) if len(parents) > 0 else 0
            if level not in self.prime_units_by_level:
                self.prime_units_by_level[level] = []
            visible_prime_units = set()
            for V in self.prime_units_by_level[level]:
                if self.is_reachable(V, name) and (V != name):
                    visible_prime_units.add(self.pids[V])
            if 3*len(visible_prime_units) >= 2*self.n_processes:
                level = level + 1
        else:
            level = level_hint

        self.levels[name] = level
        self.update_prime_units(name)

        height = self.compute_node_height(name)
        self.add_node_info(name, "height", height)

        if aux_info is not None:
            for key, val in aux_info.items():
                self.add_node_info(name, key, val)

        self.nodes_as_added.append(name)


    def is_reachable(self, U, V):
        '''
        Checks whether V is reachable from U in the dag, using a simple graph search algorithm.
        :returns: a boolean value True if reachable, False otherwise
        '''

        if self.no_forkers:
            return self.fast_is_reachable(U, V)

        have_path_to_V = set()
        node_head = set([V])
        while node_head:
            node  = node_head.pop()
            if node == U:
                return True

            if node not in have_path_to_V:
                have_path_to_V.add(node)

                for parent in self.parents(node):
                    if parent in have_path_to_V:
                        continue
                    node_head.add(parent)

        return False

    def fast_is_reachable(self, U, V):
        '''
        Same as is_reachable but optimized for the case when there are no forkers in the dag.
        Assumes that the levels of both U and V are available.
        '''
        assert U in self.levels and V in self.levels, "One of the nodes does not have a level calculated."
        have_path_to_V = set()
        node_head = set([V])
        U_pid = self.pids[U]
        U_height = self.height(U)
        while node_head:
            node  = node_head.pop()
            # optimization that takes advantage of the fact that there are no forks
            if self.pid(node) == U_pid:
                if self.height(node) >= U_height:
                    return True

            if node not in have_path_to_V:
                have_path_to_V.add(node)
                # optimization: if we reached too low of a level, no need to go deeper
                if self.levels[node] < self.levels[U]:
                    continue
                for parent in self.parents(node):
                    if parent in have_path_to_V:
                        continue
                    node_head.add(parent)

        return False


    def nodes_below(self, arg):
        '''
        Finds the set of all nodes U that are below arg (either a single node or a set of nodes).
        '''
        ret = set()
        node_head = set([arg]) if isinstance(arg, str) else set(arg)
        while node_head:
            node  = node_head.pop()
            if node not in ret:
                ret.add(node)
                for parent_node in self.parents(node):
                    if parent_node in ret:
                        continue
                    node_head.add(parent_node)

        return ret


    def self_predecessor(self, pid, parent_nodes):
        '''
        Computes the self predecessor of a node. Because the node might not yet be in the poset, only its creator_id and its parents are provided.
        The self_predecessor of U is defined as the unique maximal element in the set of all units below U creater by the same process as U.
        NOTE: currently all implementations only create units with their first parent as the self predecessor.
        :param int pid: the creator_id of the node
        :param list parent_nodes: the list of parents of the node
        :returns: the self_predecessor of the node, or None if it does not exist
        '''
        if self.no_forkers:
            if parent_nodes:
                return parent_nodes[0]
            else:
                return None

        parent_nodes = list(parent_nodes)
        below_within_process = [node_below for node_below in self.nodes_below(parent_nodes) if self.pid(node_below) == pid]

        if len(below_within_process) == 0:
            return None
        list_maximal = self.compute_maximal_from_subset(below_within_process)
        if len(list_maximal) != 1:
            return None
        if parent_nodes[0] != list_maximal[0]:
            return None
        return list_maximal[0]


    def compute_maximal_from_subset(self, subset):
        '''
        :returns: the set of maximal elements in a subset of nodes of the dag.
        '''
        parents = set()
        for node in subset:
            parents.update(self.parents(node))
        return list(set(subset) - self.nodes_below(parents))


    def maximal_units_per_process(self, process_id):
        '''
        :returns: the set of maximal nodes in the dag, among created be process_id. If this process is not forking, it should be at most one node.
        '''
        return self.compute_maximal_from_subset([U for U in self if self.pid(U) == process_id])

    def floor(self, U):
        '''
        :returns: the floor of the node U, i.e. for every process_id a list of maximal units by process_id that are below U.
        '''
        lower_cone = self.nodes_below(U)
        return [self.compute_maximal_from_subset([V for V in lower_cone if self.pid(V) == process_id]) for process_id in range(self.n_processes)]


    def sorted(self):
        '''
        :returns: a list of all units in dag, topologically sorted
        '''
        children = {}
        for node in self:
            children[node] = 0

        for node, parents in self.nodes.items():
            for parent in parents:
                children[parent] += 1

        childless = []
        for node in self:
            if children[node] == 0:
                childless.append(node)

        ret = []
        while childless:
            node = childless.pop()
            ret.append(node)
            for parent in self.parents(node):
                children[parent] -= 1
                if children[parent] == 0:
                    childless.append(parent)
        return list(reversed(ret))
