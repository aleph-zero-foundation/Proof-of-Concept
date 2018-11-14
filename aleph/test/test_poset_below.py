from aleph.data_structures import Unit, Poset
from aleph.utils import dag_utils

def test_trivial_single_level_below():

    dag_utils.is_reachable(...)
    
    n_processes = 4
    genesis_unit = Unit(creator_id = None, parents = [], txs = [])
    poset = Poset(n_processes = n_processes, process_id = 0, genesis_unit = genesis_unit,
                        secret_key = None, public_key = None)
    
    bottom_units_per_process = [Unit(creator_id = i, parents = [genesis_unit], txs = []) for i in range(n_processes)]
    for i in range(n_processes):
        poset.add_unit(bottom_units_per_process[i])
    
    for i in range(n_processes):
        assert poset.max_units_per_process[i] is bottom_units_per_process[i]
        
    U0 = poset.max_units_per_process[0]
    U1 = poset.max_units_per_process[1]
    U2 = poset.max_units_per_process[2]
    U3 = poset.max_units_per_process[3]
    
    U = Unit(creator_id = 0, parents = [U0, U1], txs = [])
    poset.add_unit(U)
    assert poset.below(U0, U)
    assert poset.above(U, U0)
    assert poset.below(U1, U)
    assert not poset.below(U2, U)
    assert not poset.below(U3, U)