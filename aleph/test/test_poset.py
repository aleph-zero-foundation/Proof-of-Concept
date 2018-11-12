from aleph.data_structures import Unit, Poset

def test_create_poset():
    n_processes = 4
    genesis_unit = Unit(creator_id = None, parents = [], txs = [])
    aleph_poset = Poset(n_processes = n_processes, process_id = 0, genesis_unit = genesis_unit,
                        secret_key = None, public_key = None)
    
    bottom_units_per_process = [Unit(creator_id = i, parents = [genesis_unit], txs = []) for i in range(n_processes)]
    for i in range(n_processes):
        aleph_poset.add_unit(bottom_units_per_process[i])
    
    for i in range(n_processes):
        assert aleph_poset.max_units_per_process[i] is bottom_units_per_process[i]
        
    U0 = aleph_poset.max_units_per_process[0]
    U1 = aleph_poset.max_units_per_process[1]
    U = Unit(creator_id = 0, parents = [U0, U1], txs = [])
    
    aleph_poset.add_unit(U)
    
    