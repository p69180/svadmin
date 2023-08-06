import utils
import pprint


def main():
    #tgroup_list = utils.run_ps()
    #print(len(tgroup_list))

    tgroups, tmerge_df = utils.run_ps_new()
    return tgroups, tmerge_df
    

if __name__ == '__main__':
    main()
