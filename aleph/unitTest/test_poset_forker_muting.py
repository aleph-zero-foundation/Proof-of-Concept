from aleph.data_structures import Unit, Poset
from aleph.utils import dag, dag_utils
import random


def check_muting_vs_pattern(dag, topological_list, pattern):
    unit_dict = {}
    poset = Poset(n_processes = dag.n_processes)

    for node, answer in zip(topological_list, pattern):
        U = Unit(creator_id = dag.pid(node), parents = [unit_dict[parent] for parent in dag.parents(node)], txs = [])
        poset.set_self_predecessor_and_height(U)
        unit_dict[node] = U
        poset.prepare_unit(U)
        assert poset.check_forker_muting(U) == answer
        poset.add_unit(U)



def test_small_random_muting():
    random.seed(123456789)
    repetitions = 800
    for rep in range(repetitions):
        n_processes = random.randint(4, 15)
        n_units = random.randint(0, n_processes*2)
        n_forkers = random.randint(1, n_processes)
        constraints_ensured = {'forker_muting' : True}
        constraints_violated = {'forker_muting'  : False}
        dag, topological_list = dag_utils.generate_random_violation(n_processes, n_units, n_forkers,
                                constraints_ensured, constraints_violated)
        pattern = [True] * len(topological_list)
        pattern[-1] = False
        check_muting_vs_pattern(dag, topological_list, pattern)



def test_large_random_muting():
    random.seed(123456789)
    repetitions = 20
    for rep in range(repetitions):
        n_processes = random.randint(50, 70)
        n_units = random.randint(0, n_processes*2)
        n_forkers = random.randint(1, 5)
        constraints_ensured = {'forker_muting' : True}
        constraints_violated = {'forker_muting'  : False}
        dag, topological_list = dag_utils.generate_random_violation(n_processes, n_units, n_forkers,
                                constraints_ensured, constraints_violated)

        pattern = [True] * len(topological_list)
        pattern[-1] = False
        check_muting_vs_pattern(dag, topological_list, pattern)
