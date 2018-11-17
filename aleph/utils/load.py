from aleph.data_structures import Unit, Poset
from aleph.utils import dag_utils

def load(file_name):
    '''
    Loads a poset from a file and returns a Poset instance along with a dictionary mapping 
    unit names to units in the returned poset.
    :param string file_name: the path to the file with a poset
    :returns: a pair (poset, name_mapping) where poset is the resulting Poset instance 
    and name_mapping is a dictionary unit_name -> Unit 
    '''
    dag, n_processes = dag_utils.dag_from_file(file_name)
    
    poset_from_file, unit_dict = dag_utils.poset_from_dag(dag, n_processes)

    return poset_from_file, unit_dict



