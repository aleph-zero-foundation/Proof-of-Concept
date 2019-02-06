from aleph.data_structures import Unit, Poset
from aleph.utils import dag, dag_utils
import random


def check_growth_vs_pattern(dag, topological_list, pattern):
    unit_dict = {}
    poset = Poset(n_processes = dag.n_processes)

    for node, answer in zip(topological_list, pattern):
        U = Unit(creator_id = dag.pid(node), parents = [unit_dict[parent] for parent in dag.parents(node)], txs = [])
        unit_dict[node] = U
        poset.prepare_unit(U)
        assert poset.check_growth(U) == answer
        poset.add_unit(U)



def test_small_random_growth():
    random.seed(123456789)
    repetitions = 2000
    for i in range(repetitions):
        n_processes = random.randint(4, 15)
        n_units = random.randint(0, n_processes*5)
        n_forkers = random.randint(0, n_processes)
        constraints_ensured = {'growth' : True}
        constraints_violated = {'growth' : False}
        dag, topological_list = dag_utils.generate_random_violation(n_processes, n_units, n_forkers,
                                constraints_ensured, constraints_violated)
        pattern = [True] * len(topological_list)
        pattern[-1] = False
        check_growth_vs_pattern(dag, topological_list, pattern)



def test_large_random_growth():
    random.seed(123456789)
    repetitions = 20
    for i in range(repetitions):
        n_processes = random.randint(50, 100)
        n_units = random.randint(0, n_processes*3)
        n_forkers = random.randint(0, 5)
        constraints_ensured = {'growth' : True}
        constraints_violated = {'growth' : False}
        dag, topological_list = dag_utils.generate_random_violation(n_processes, n_units, n_forkers,
                                constraints_ensured, constraints_violated)
        pattern = [True] * len(topological_list)
        pattern[-1] = False
        check_growth_vs_pattern(dag, topological_list, pattern)
