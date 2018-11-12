def topsort(poset):
    for unit in poset.units.values():
        unit.children = 0

    for unit in poset.units.values():
        for parent in unit.parents:
            parent.children += 1

    childless = []
    for unit in poset.units.values():
        if unit.children == 0:
            childless.append(unit)

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
    assert toporder[-1] is poset.genesis_unit, "Genesis unit is not last in topological order"

    for i,unit in enumerate(toporder):
        unit.name = i
    toporder[-1].name = genesis

    with open(filename, 'w') as f:
        f.write('{} {}\n'. format(poset.n_processes, genesis))
        for unit in toporder:
            s = '{} {}'.format(unit.name, unit.creator_id)
            for parent in unit.parents:
                s += ' {}'.format(parent.name)
            s += '\n'
            f.write(s)




