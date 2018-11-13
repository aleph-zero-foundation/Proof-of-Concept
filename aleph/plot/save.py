def topsort(poset):
    for U in poset.Us.values():
        U.children = 0

    for U in poset.Us.values():
        for parent in U.parents:
            parent.children += 1

    childless = []
    for U in poset.Us.values():
        if U.children == 0:
            childless.append(U)

    ret = []
    while childless:
        U = childless.pop()
        del U.children
        ret.append(U)
        for parent in U.parents:
            parent.children -= 1
            if parent.children == 0:
                childless.append(parent)
    return ret


def save(poset, filename, genesis='G'):
    toporder = topsort(poset)
    assert toporder[-1] is poset.genesis_U, "Genesis U is not last in topological order"

    for i, U in enumerate(toporder):
        U.name = i
    toporder[-1].name = genesis

    with open(filename, 'w') as f:
        f.write('{} {}\n'. format(poset.n_processes, genesis))
        for U in toporder:
            line = '{} {}'.format(U.name, U.creator_id)
            for parent in U.parents:
                line += ' {}'.format(parent.name)
            line += '\n'
            f.write(line)
