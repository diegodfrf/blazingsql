def has_cudf():
  try: # TODO Percy Tom Cordova improve this approach
    import cudf
    import dask_cudf
  except ImportError as error:
    # If one of these are not installed yet will return False 
    return False
  return True

def has_dask():
  try: # TODO Percy Tom Cordova improve this approach
    import dask.distributed
    import dask
  except ImportError as error:
    return False
  return True


def has_hive():
  try: # TODO Percy Tom Cordova improve this approach
    from pyhive import hive
  except ImportError as error:
    return False
  return True
