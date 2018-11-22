from aleph.data_structures import Unit, Poset
from aleph.utils import dag_utils
import random


def check_diversity_vs_pattern(dag, topological_list, pattern):
    unit_dict = {}
    poset = Poset(n_processes = dag.n_processes, process_id = 0, secret_key = None, public_key = None)

    for node, answer in zip(topological_list, pattern):
        U = Unit(creator_id = dag.pid(node), parents = [unit_dict[parent] for parent in dag.parents(node)], txs = [])
        poset.set_self_predecessor_and_height(U)
        unit_dict[node] = U
        assert poset.check_parent_diversity(U) == answer
        poset.add_unit(U)




def test_small_random_diversity():
    random.seed(123456789)
    repetitions = 2000
    for i in range(repetitions):
        n_processes = random.randint(4, 15)
        n_units = random.randint(0, n_processes*5)
        n_forkers = random.randint(0, n_processes)
        constraints_ensured = {'parent_diversity' : True}
        constraints_violated = {'parent_diversity' : False}
        dag, topological_list = dag_utils.generate_random_violation(n_processes, n_units, n_forkers,
                                constraints_ensured, constraints_violated)
        pattern = [True] * len(topological_list)
        pattern[-1] = False
        check_diversity_vs_pattern(dag, topological_list, pattern)


def test_large_random_diversity():
    random.seed(123456789)
    repetitions = 20
    for i in range(repetitions):
        n_processes = random.randint(50, 100)
        n_units = random.randint(0, n_processes*3)
        n_forkers = random.randint(0, 5)
        constraints_ensured = {'parent_diversity' : True}
        constraints_violated = {'parent_diversity' : False}
        dag, topological_list = dag_utils.generate_random_violation(n_processes, n_units, n_forkers,
                                constraints_ensured, constraints_violated)

        pattern = [True] * len(topological_list)
        pattern[-1] = False
        check_diversity_vs_pattern(dag, topological_list, pattern)