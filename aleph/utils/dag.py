import collections
import copy
import functools

from aleph.data_structures import Poset, Unit



class memo:
    '''Decorator. Caches a function's return value each time it is called.
    If called later with the same arguments, the cached value is returned
    (not reevaluated).
    '''
    def __init__(self, func):
        self.func = func
        self.cache = {}
    def __call__(self, *args):
        if args in self.cache:
            return self.cache[args]
        else:
            value = self.func(*args)
            self.cache[args] = value
            return value
    def __get__(self, obj, objtype):
        '''Support instance methods.'''
        return functools.partial(self.__call__, obj)



class DAG:

    def __init__(self, n_processes):
        self.n_processes = n_processes
        self.nodes = {}
        self.pids = {}
        self.levels = {}
        self.prime_units_by_level = {}


    def __contains__(self, node): return node in self.nodes
    def __iter__(self): return iter(self.nodes)
    def __len__(self): return len(self.nodes)

    def pid(self, node): return self.pids[node]
    def parents(self, node): return iter(self.nodes[node])


    def add(self, name, pid, parents):
        assert all(p in self for p in parents), 'Parents of {} are not in DAG'.format(name)
        self.pids[name] = pid
        self.nodes[name] = parents[:]
        level = max([self.levels[p] for p in parents]) if len(parents) > 0 else 0
        if level not in self.prime_units_by_level:
            self.prime_units_by_level[level] = []
        visible_prime_units = set()
        for V in self.prime_units_by_level[level]:
            if self.is_reachable(V, name) and (V != name):
                visible_prime_units.add(self.pids[V])
        if 3*len(visible_prime_units) >= 2*self.n_processes:
            level = level + 1
        if level not in self.prime_units_by_level:
            self.prime_units_by_level[level] = []
        self.levels[name] = level
        predecessor = self.self_predecessor(pid, parents)
        if predecessor is None or level > self.levels[predecessor]:
            self.prime_units_by_level[level].append(name)




    @memo
    def is_reachable(self, U, V):
        '''Checks whether V is reachable from U in a DAG, using BFS
        :param dict dag: a dictionary of the form: node -> [list of parent nodes]
        :returns: a boolean value True if reachable, False otherwise
        '''
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
    below = is_reachable


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
        parents = set()
        for node in subset:
            parents.update(self.parents(node))
        return list(set(subset) - self.nodes_below(parents))


    def maximal_units_per_process(self, process_id):
        return self.compute_maximal_from_subset([U for U in self if self.pid(U) == process_id])

    def floor(self, U):
        lower_cone = self.nodes_below(U)
        return [self.compute_maximal_from_subset([V for V in lower_cone if self.pid(V) == process_id]) for process_id in range(self.n_processes)]


    def sorted(self):
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
