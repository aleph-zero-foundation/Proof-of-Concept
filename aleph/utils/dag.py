import copy

from aleph.data_structures import Poset, Unit


class DAG:

    def __init__(self, n_processes):
        self.n_processes = n_processes
        self.nodes = {}
        self.pids = {}


    def __contains__(self, node): return node in self.nodes
    def __iter__(self): return iter(self.nodes)
    def __len__(self): return len(self.nodes)


    def pid(self, node):
        return self.pids[node]


    def add(self, name, pid, parents):
        assert all(p in self for p in parents), 'Parents of {} are not in DAG'.format(name)

        self.pids[name] = pid
        self.nodes[name] = parents[:]


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
                for parent_node in self.nodes[node]:
                    if parent_node in have_path_to_V:
                        continue
                    node_head.add(parent_node)

        return False


    def nodes_below(self, V):
        '''
        Finds the set of all nodes U that have a path from U to V.
        '''
        have_path_to_V = set()
        node_head = set([V])
        while node_head:
            node  = node_head.pop()
            if node not in have_path_to_V:
                have_path_to_V.add(node)
                for parent_node in self.nodes[node]:
                    if parent_node in have_path_to_V:
                        continue
                    node_head.add(parent_node)

        return have_path_to_V


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
            for parent in self.nodes[node]:
                children[parent] -= 1
                if children[parent] == 0:
                    childless.append(parent)
        return list(reversed(ret))


    def reversed(self):
        ret = DAG(self.n_processes)
        ret.pids = copy.copy(self.pids)
        for node, parents in self.nodes.items():
            for parent in parents:
                if parent not in ret.nodes:
                    ret.nodes[parent] = []
                ret.nodes[parent].append(node)
        return ret



#======================================================================================================================
#======================================================================================================================
#======================================================================================================================



def poset_from_dag(dag):
    poset = Poset(n_processes = dag.n_processes, process_id = 0, secret_key = None, public_key = None)
    unit_dict = {}

    for unit_name in dag.sorted():
        creator_id = dag.pid(unit_name)
        assert 0 <= creator_id <= n_processes - 1, "Incorrect process id"

        parents = dag.nodes[node]
        assert unit_name not in unit_dict, "Duplicate unit name %s" % unit_name
        for parent in parents:
            assert parent in unit_dict, "Parent %s of unit %s not known" % (parent, unit_name)

        U = Unit(creator_id = unit_creator_id, parents = [unit_dict[parent] for parent in parents],
                txs = [])
        poset.add_unit(U)
        unit_dict[unit_name] = U

    return poset, unit_dict



def create_node_line(node, process_id, parents):
    line = '%s %d' % (node, process_id)
    for parent in parents:
        line += ' ' + parent
    line += '\n'
    return line



def dag_to_file(dag, file_name):
    topological_list = dag.sorted()
    with open(file_name, 'w') as f:
        f.write('%d\n' % dag.n_processes)
        for node in topological_list:
            f.write(create_node_line(node, dag.pid(node), dag.nodes[node]))



def dag_from_file(file_name):
    with open(file_name) as poset_file:
        lines = poset_file.readlines()

    n_processes = int(lines[0])
    dag = DAG(n_processes)

    for line in lines[1:]:
        tokens = line.split()
        unit_name = tokens[0]
        creator_id = int(tokens[1])
        assert 0 <= creator_id <= n_processes - 1, "Incorrect process id"
        parents = tokens[2:]
        assert unit_name not in dag, "Duplicate unit name %s" % unit_name
        for parent in parents:
            assert parent in dag, "Parent %s of a unit %s not known" % (parent, unit_name)

        dag.add(unit_name, creator_id, parents)

    return dag



