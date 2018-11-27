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


    def __contains__(self, node): return node in self.nodes
    def __iter__(self): return iter(self.nodes)
    def __len__(self): return len(self.nodes)

    def pid(self, node): return self.pids[node]
    def parents(self, node): return iter(self.nodes[node])


    def add(self, name, pid, parents):
        assert all(p in self for p in parents), 'Parents of {} are not in DAG'.format(name)
        self.pids[name] = pid
        self.nodes[name] = parents[:]


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


    @memo
    def count_support(self, U, V):
        below_V = self.nodes_below(V)
        above_U = self.reversed().nodes_below(U)
        return len({self.pid(node) for node in (below_V & above_U)})


    def is_reachable_through_n_intermediate(self, U, V, n):
        '''
        Checks whether the number of different processes that have units on some path
        from U to V in dag is at least n.
        '''
        return self.count_support(U, V) >= n


    def self_predecessor(self, pid, parent_nodes):
        below_within_process = [node_below for node_below in self.nodes_below(parent_nodes) if self.pid(node_below) == pid]

        if len(below_within_process) == 0:
            return None
        list_maximal = self.compute_maximal_from_subset(below_within_process)
        if len(list_maximal) != 1:
            return None
        return list_maximal[0]


    def compute_maximal_from_subset(self, subset):
        parents = set()
        for node in subset:
            parents.update(self.parents(node))
        return list(set(subset) - self.nodes_below(parents))


    def old_compute_maximal_from_subset(self, subset):
        maximal_from_subset = []
        for U in subset:
            is_maximal = True
            for V in subset:
                if V is not U and self.is_reachable(U, V):
                    is_maximal = False
                    break
            if is_maximal:
                maximal_from_subset.append(U)
        return maximal_from_subset


    def maximal_units_per_process(self, process_id):
        return self.compute_maximal_from_subset([U for U in self if self.pid(U) == process_id])


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


    def reversed(self):
        ret = DAG(self.n_processes)
        ret.pids = copy.copy(self.pids)
        for node in self:
            ret.nodes[node] = []
        for node in self:
            for parent in self.parents(node):
                ret.nodes[parent].append(node)
        return ret





