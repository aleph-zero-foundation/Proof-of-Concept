from aleph.data_structures import Unit, Poset
from aleph.utils import dag_utils
import random


def check_growth_vs_pattern(dag, topological_list, n_processes, pattern):
    unit_dict = {}
    poset = Poset(n_processes = n_processes, process_id = 0, secret_key = None, public_key = None)

    for node, answer in zip(topological_list, pattern):
        unit_name, unit_creator_id = node
        parents = dag[node]

        U = Unit(creator_id = unit_creator_id, parents = [unit_dict[parent[0]] for parent in parents], txs = [])
        poset.set_self_predecessor_and_height(U)
        assert poset.check_growth(U) == answer
        poset.add_unit(U)
        unit_dict[unit_name] = U



def test_small_random_growth():
    random.seed(123456789)
    n_processes = 5
    n_units = 20
    n_forkers = 1
    repetitions = 30
    for _ in range(repetitions):
        dag, topological_list = dag_utils.generate_random_growth_violation(n_processes, n_units, n_forkers)
        pattern = [True] * len(topological_list)
        pattern[-1] = False
        check_growth_vs_pattern(dag, topological_list, n_processes, pattern)