from aleph.data_structures import Unit, Poset
import random


def create_unit_line(unit_name, process_id, parent_names):
    line = '%s %d ' % (unit_name, process_id)
    for name in parent_names:
        line = line + name + ' '
    return line
    
def generate_unit_name(unit_height, process_id):
    name = "U_%d_%d" % (unit_height, process_id)
    return name

def generate_random_nonforking(n_processes, n_units, file_name):
    '''
    Generates a random non-forking poset with n_processes processes and saves it to file_name. 
    Does not return any value.
    :param int n_processes: the number of processes in poset
    :param int n_units: the number of units in the process beyond genesis + n_processes initial units,
    hence the total number of units is (1 + n_processes + n_units)
    '''
    process_heights = [0] * n_processes
    file = open(file_name, 'w+')
    file.write('%d %s\n' % (n_processes, 'GENESIS'))
    for process_id in range(n_processes):
        file.write('U_0_%d %d GENESIS\n' % (process_id, process_id))
    
    for _ in range(n_units):
        process_id = random.sample(range(n_processes), 1)[0]
        all_but_process_id = list(range(n_processes))
        all_but_process_id.remove(process_id)
        parent_processes = [process_id] + random.sample(all_but_process_id , 1)
        unit_height = process_heights[process_id] + 1
        unit_name = generate_unit_name(unit_height, process_id)
        parent_names = [generate_unit_name(process_heights[id], id) for id in parent_processes]
        file.write(create_unit_line(unit_name, process_id, parent_names) + '\n')
        
        process_heights[process_id] += 1
        
    file.close()
    
if __name__ == "__main__":    
    generate_random_nonforking(10, 30, 'random_10_30.txt')
    