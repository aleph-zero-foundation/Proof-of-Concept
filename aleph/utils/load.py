from aleph.data_structures import Unit, Poset

def load(file_name):
    '''
    Loads a poset from a file and returns a Poset instance along with a dictionary mapping 
    unit names to units in the returned poset.
    :param string file_name: the path to the file with a poset
    :returns: a pair (poset, name_mapping) where poset is the resulting Poset instance 
    and name_mapping is a dictionary unit_name -> Unit 
    '''
    poset_from_file = None
    with open(file_name) as poset_file:
        lines = poset_file.readlines()

    unit_dict = {}
    head_line = lines[0]
    n_processes = head_line.split()
    n_processes = int(n_processes)
    unit_dict[genesis_name] = genesis_unit
    poset_from_file = Poset(n_processes = n_processes, process_id = 0,
                            secret_key = None, public_key = None)

    for line in lines[1:]:
        tokens = line.split()
        unit_name = tokens[0]
        #print(unit_name)
        unit_creator_id = int(tokens[1])
        assert 0 <= unit_creator_id <= n_processes - 1, "Incorrect process id"
        parents = tokens[2:]
        assert unit_name not in unit_dict.keys(), "Duplicate unit name %s" % unit_name
        for parent in parents:
            assert parent in unit_dict.keys(), "Parent %s of a unit %s not known" % (parent, unit_name)

        U = Unit(creator_id = unit_creator_id, parents = [unit_dict[parent] for parent in parents],
                txs = [])
        poset_from_file.add_unit(U)
        unit_dict[unit_name] = U

    return poset_from_file, unit_dict



