import re
import operator
import itertools
import subprocess
import pprint
import argparse
import random

import utils as utils


NAME_PAT = re.compile(r'(?P<ip1>[^->]+):(?P<port1>[0-9]+)(->(?P<ip2>[^->]+):(?P<port2>[0-9]+))?( \((?P<type>.+)\))?')
SUBCMD_USEDPORTS = 'usedports'
SUBCMD_GETPORT = 'getport'


def parse_arguments():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(
        title='sub-commands',
        dest='subcommand',
        required=True,
    )

    # listing used ports
    parser_usedports = subparsers.add_parser(
        SUBCMD_USEDPORTS, 
        help=f'Prints all used port numbers, grouped by user',
    )

    # print a usable port
    parser_getport = subparsers.add_parser(
        SUBCMD_GETPORT, 
        help=f'Prints a single unprivileged port not currently open',
    )
    parser_getport.add_argument('-n', default=1, type=int, help=f'The number of closed ports to print')

    args = parser.parse_args()
    return args


def simplify_portinfo(portinfo):
    result = dict()
    for key in (
        'USER',
        'COMMAND',
        'PID',
        'NODE',
    ):
        result[key] = portinfo[key]

    name_mat = NAME_PAT.fullmatch(portinfo['NAME'])
    assert name_mat is not None, portinfo['NAME']

    if (name_mat.group('type') == 'ESTABLISHED') and (name_mat.group('port2') is None):
        print(portinfo['NAME'])
        print(name_mat.groupdict())
        exit()

    result['type'] = name_mat.group('type')
    result['ports'] = tuple(
        int(name_mat.group(key)) for key in ['port1', 'port2']
        if (name_mat.group(key) is not None)
    )

    return result


def group_portinfo_byuser(portinfo_list):
    return dict(
        (x[0], list(x[1])) for x in 
        itertools.groupby(
            sorted(portinfo_list, key=operator.itemgetter('USER')),
            key=operator.itemgetter('USER'),
        )
    )


def parse_lsof_output(output):
    # raw port information
    stdout_split = output.rstrip().split('\n')
    header = stdout_split[0].split()
    portinfo_list = list()
    for line in stdout_split[1:]:
        linesp = line.split()
        if len(linesp) == 10:
            linesp[8] = linesp[8] + ' ' + linesp[9]
            del linesp[9]
        portinfo = dict(zip(header, linesp))
        portinfo_list.append(portinfo)

    # simplified ones
    portinfo_simple_list = [simplify_portinfo(x) for x in portinfo_list]
    
    # byuser
    portinfo_byuser = group_portinfo_byuser(portinfo_list)
    portinfo_simple_byuser = group_portinfo_byuser(portinfo_simple_list)

    return portinfo_list, portinfo_simple_list, portinfo_byuser, portinfo_simple_byuser


def get_all_used_ports(portinfo_simple_list):
    result = set()
    for x in portinfo_simple_list:
        result.update(x['ports'])
    return result


def get_unprivileged_port_range():
    stdout = subprocess.check_output('sysctl -n net.ipv4.ip_local_port_range', shell=True, text=True)
    start1, end1 = stdout.split()
    start1 = int(start1)
    end1 = int(end1)
    return range(start1, end1 + 1)


def main():
    args = parse_arguments()

    utils.rootcheck_ask_force()
    p = subprocess.run('lsof +c 0 -i -n -P', shell=True, text=True, capture_output=True)
    portinfo_list, portinfo_simple_list, portinfo_byuser, portinfo_simple_byuser = parse_lsof_output(p.stdout)
    all_used_ports = get_all_used_ports(portinfo_simple_list)

    if args.subcommand == SUBCMD_USEDPORTS:
        for user in sorted(portinfo_simple_byuser.keys()):
            print(user)
            for x in portinfo_simple_byuser[user]:
                port_string = " ,".join(map(str, x["ports"]))
                print(port_string, x['COMMAND'], sep='\t')
            print()
    elif args.subcommand == SUBCMD_GETPORT:
        unprivileged_ports = set(get_unprivileged_port_range())
        usable_ports = tuple(unprivileged_ports.difference(all_used_ports))
        result = sorted(random.sample(usable_ports, args.n))
        for x in result:
            print(x)


if __name__ == '__main__':
    main()
    

