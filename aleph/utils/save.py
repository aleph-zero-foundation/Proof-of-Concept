def topsort(poset):
    children = {}
    for U in poset.units.values():
        children[U] = 0

    for U in poset.units.values():
        for parent in U.parents:
            children[parent] += 1

    childless = []
    for U in poset.units.values():
        if children[U] == 0:
            childless.append(U)

    ret = []
    while childless:
        U = childless.pop()
        ret.append(U)
        for parent in U.parents:
            children[parent] -= 1
            if children[parent] == 0:
                childless.append(parent)
    return list(reversed(ret))


def save(poset, filename, genesis='G'):
    toporder = topsort(poset)
    assert toporder[0] is poset.genesis_unit, "Genesis U is not last in topological order"

    names = {}
    names[toporder[0]] = genesis
    for i, U in enumerate(toporder[1:]):
        names[U] = i

    with open(filename, 'w') as f:
        f.write('{} {}\n'. format(poset.n_processes, genesis))
        for U in toporder[1:]:
            line = '{} {}'.format(names[U], U.creator_id)
            for parent in U.parents:
                line += ' {}'.format(names[parent])
            line += '\n'
            f.write(line)
