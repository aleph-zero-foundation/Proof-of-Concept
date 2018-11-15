from aleph.data_structures import Unit, Poset
from aleph.utils import dag_utils
from aleph.utils import generate_poset
import random

def test_trivial_single_level_below():
    n_processes = 4
    poset = Poset(n_processes = n_processes, process_id = 0, secret_key = None, public_key = None)
    
    bottom_units_per_process = [Unit(creator_id = i, parents = [], txs = []) for i in range(n_processes)]
    for i in range(n_processes):
        poset.add_unit(bottom_units_per_process[i])
    
    for i in range(n_processes):
        assert poset.max_units_per_process[i][0] is bottom_units_per_process[i]
        
    U0 = poset.max_units_per_process[0][0]
    U1 = poset.max_units_per_process[1][0]
    U2 = poset.max_units_per_process[2][0]
    U3 = poset.max_units_per_process[3][0]
    
    U = Unit(creator_id = 0, parents = [U0, U1], txs = [])

    poset.add_unit(U)
    assert poset.below(U0, U)
    assert poset.above(U, U0)
    assert poset.below(U1, U)
    assert not poset.below(U2, U)
    assert not poset.below(U3, U)
    
    
def test_small_nonforking_below():
    random.seed(123456789)
    n_processes = 5
    n_units = 50
    dag = generate_poset.generate_random_nonforking(n_processes, n_units)
    check_all_pairs_below(dag, n_processes)
            
            
def test_small_forking_below():
    random.seed(123456789)
    n_processes = 5
    n_units = 9
    n_forking = 1
    dag = generate_poset.generate_random_forking(n_processes, n_units, n_forking)
    dag_utils.dag_to_file(dag, n_processes, 'bad_test.txt')
    check_all_pairs_below(dag, n_processes)
    
    
def check_all_pairs_below(dag, n_processes):
    '''
    Create a poset from a dag and test U<=V for all pairs of units U,V
    against a naive BFS-implementation from dag_utils
    '''
    poset, unit_dict = dag_utils.poset_from_dag(dag, n_processes)
    
    for nameU, U in unit_dict.items():
        for nameV, V in unit_dict.items():
            nodeU = (nameU, U.creator_id)
            nodeV = (nameV, V.creator_id)
            assert poset.below(U,V) == dag_utils.is_reachable(dag, nodeU, nodeV)
            assert poset.above(U,V) == dag_utils.is_reachable(dag, nodeV, nodeU)
    
    
#dag1 = generate_poset.generate_random_nonforking(5, 5, 'example1.txt')
#dag2 = generate_poset.generate_random_forking(5, 10, 2, 'example2.txt')