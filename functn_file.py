def start_fun(sc, filenames):
    print (filenames)
    debugging = False
    real_impute_NOT_param_estimate = True
    use_spark = True # otherwise, will use joblib
    import sys
    if('ipykernel_launcher' in sys.argv[0]):
        using_jupyter_gui = True
    else:
        using_jupyter_gui = False


    # custom lib
    import pandas as pd
    import numpy as np
    import scipy as sp
    from joblib import Parallel, delayed

    # build-in lib
    import time
    from datetime import datetime
    import math

    import warnings
    import itertools
    import statsmodels.api as sm

    if(using_jupyter_gui):
        import matplotlib.pyplot as plt
        #plt.style.use('ggplot')
        plt.style.use('fivethirtyeight')

    #exp_started_datetime_string = datetime.now().strftime("%Y%m%d_%H%M%S")
    exp_started_datetime_string = datetime.now().strftime("%Y%m%d_%H%M")

    filename_prefix = 'started_GMT_' + exp_started_datetime_string
    if(debugging):
        filename_prefix = 'debugging-' + filename_prefix
    filename_data = filename_prefix + '.npy'
    filename_log = 'log/' + filename_prefix + '.log.txt'
    print(filename_prefix)

    # redirect output print to a log file
    if(not using_jupyter_gui):
        import sys
        orig_stdout = sys.stdout
        f_stdout = open(filename_log, 'w')
        sys.stdout = f_stdout

    import inspect, os
    print(inspect.getfile(inspect.currentframe())) # script filename (usually with path)
    print(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))) # script directory

    import os
    print(os.path.abspath(inspect.stack()[0][1]))


    # Config Some Env Varialbes
    if(use_spark): # RDD partitions.
        partition_nr = min(int(3.5 * sc.defaultParallelism), 1000)
        print("sc.defaultParallelism: %d (cores), partition_nr (max, if needed): %d." % (sc.defaultParallelism, partition_nr))
    if(not use_spark): # parallel CPU threads.
        import os
        def get_nr_cpu_threads():
            #for Linux, Unix and MacOS
            if hasattr(os, "sysconf"):
                if os.sysconf_names.has_key("SC_NPROCESSORS_ONLN"):
                    #Linux and Unix
                    ncpus = os.sysconf("SC_NPROCESSORS_ONLN")
                    if isinstance(ncpus, int) and ncpus > 0:
                        return ncpus
                else:
                    #MacOS X
                    return int(os.popen2("sysctl -n hw.ncpu")[1].read())
            #for Windows
            if os.environ.has_key("NUMBER_OF_PROCESSORS"):
                ncpus = int(os.environ["NUMBER_OF_PROCESSORS"])
                if ncpus > 0:
                    return ncpus
            #return the default value
            return 1

        print("cpu_threads: %d" % get_nr_cpu_threads())


    import cv2
    import os
    import time

    pic_folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))) + '/'

    def grayscale(i):
        image = cv2.imread(pic_folder + str(i) + ".jpg")
        gray_image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        #cv2.imwrite("/home/raghu/Pictures/" + str(i) + "output.jpg", gray_image)
        cv2.waitKey(0)
        result = gray_image
        cv2.imwrite(pic_folder + str(i) + "output.jpg", result)
        return(gray_image)


    train_index_array = filenames
    #for i in train_index_array:
    #	result = grayscale(i)
    #	cv2.imwrite(pic_folder + str(i) + "output.jpg", result)

    partition_nr = min(partition_nr, len(train_index_array))
    my_rdd = sc.parallelize(list(train_index_array), partition_nr)
    map_result = my_rdd.map(grayscale)
    all_points_all_tuples_result_listing = map_result.collect()


    type(all_points_all_tuples_result_listing[0])
