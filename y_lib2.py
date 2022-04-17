import lib
import os
import io 
import time 
import glob 
import grpc 
import sys

import traceback
import shutil

from concurrent import futures
from concurrent.futures import ThreadPoolExecutor

sys.path.append('./protosrc')
sys.path.append('../')
sys.path.append('./opensfm/opensfm/')
sys.path.append('/home/pi/ODM/SuperBuild/src/opensfm')
#sys.path.append('/home/vm1/Desktop/ODM/SuperBuild/install/lib/python2.7/dist-packages')
sys.path.append('/home/pi/ODM/SuperBuild/install/lib')


import sendFile_pb2, sendFile_pb2_grpc

CHUNK_SIZE = 1024 * 1024  # 1MB

from timeit import default_timer as timer
from threading import Thread
import queue


import opensfm_interface
import collections

def start():
    print("import")

if __name__ == '__main__':
	start_time = timer()
	try:
		nodeid = 1
		nodeid_list = [1]
		node_client = {}

		node_imagelist = {}
		node_imagelist[1] = []
		node_imagelist[2] = []




		nodes_available = {2: True}
		max_concurrency = 4



		images_filepath = '/home/pi/ODM/grpc_odm/node1'  #file path of current node images
		file_path = images_filepath + '/'
		opensfm_config = opensfm_interface.setup_opensfm_config(file_path)
		active_number_of_nodes = 1
		photos_name = collections.defaultdict(lambda : "None")
		photo_list =  os.listdir(os.path.join(images_filepath, 'images'))
		print(photo_list)
		image_sent_nodes = collections.defaultdict(lambda : 'none')
		cluster_size = len(photo_list) / active_number_of_nodes # images per node
		distance_overlap = 10 # meters overlap of images
		camera_models = {}
		current_path = images_filepath

		opensfm_config = opensfm_interface.setup_opensfm_config(current_path)
		photo_list =  os.listdir(os.path.join(images_filepath, 'images'))
		image_path = os.path.join(current_path, 'images')
		distance_overlap = 10



		start = timer()

		#exif extraction
		response = lib.sfm_extract_metadata_list_of_images(image_path, opensfm_config, current_path, photo_list)


		end = timer()
		exif_extraction_time = end - start

		# feature extraction

		start = timer()
		response = lib.sfm_detect_features(photo_list, current_path  ,opensfm_config)


		end = timer()
		detect_features_time = end - start

		#feature matching

		start = timer()

		lib.sfm_feature_matching(current_path, photo_list, photo_list, opensfm_config)

		end = timer()
		feature_matching_time = end - start

		start = timer()


		response = lib.sfm_create_tracks(current_path, photo_list,opensfm_config)


		end = timer()
		create_tracks_time = end - start


		start = timer()


		# need the exifs

		lib.sfm_opensfm_reconstruction(current_path, opensfm_config)


		end = timer()
		sfm_opensfm_reconstruction_time = end - start

		start = timer()
		lib.sfm_undistort_image(current_path, opensfm_config)

		end = timer()
		sfm_undistort_image_time = end - start


		start = timer()
		lib.sfm_export_visual_sfm(current_path, opensfm_config)


		end = timer()
		sfm_export_visualsfm_time = end - start

		start = timer()

		#lib.sfm_compute_depthmaps(current_path, opensfm_config)

		end = timer()
		sfm_compute_depthmaps_time = end - start
		
		start = timer()

		max_concurrency = 4
		#need images
  
	    ### 
        ###   raise IOError(reconstruction_json + " does not exist.")


		# delete from makescene
		lib.mve_makescene_function(current_path, max_concurrency)



		end = timer()
		mve_makescene_function_time = end - start

		start = timer()

		lib.mve_dense_reconstruction(current_path ,max_concurrency)

		end = timer()
		mve_dense_reconstruction_time = end - start

		start = timer()

		lib.mve_scene2pset_function(current_path, max_concurrency)

		end = timer()
		mve_mve_scene2pset_time = end - start

		start = timer()

		lib.mve_cleanmesh_function(current_path, max_concurrency)

		end = timer()
		mve_mve_cleanmesh_time = end - start


		start = timer()


		lib.odm_filterpoints_function(current_path, max_concurrency)


		end = timer()
		odm_filterpoint_time = end - start

		start = timer()
  
        from opendm import io
        images_database_file = io.join_paths(current_path, 'images.json')
        photo_list = node_cluster_list[current_node_submodel_key]
        
        photos = []
        if not io.file_exists(images_database_file):
            files = photo_list
	        images_dir = io.join_paths(file_path,'images')
            if len(files)>0:
                # create ODMPhoto list
                path_files = [io.join_paths(images_dir, f) for f in files]

                
		        dataset_list = io.join_paths(file_path,'img_list')
                with open(dataset_list, 'w') as dataset_list:
                    log.ODM_INFO("Loading %s images" % len(path_files))
                    for f in path_files:
                        photos += [types.ODM_Photo(f)]
                        dataset_list.write(photos[-1].filename + '\n')

                # Save image database for faster restart
                lib.save_images_database(photos, images_database_file)
            else:
                log.ODM_ERROR('Not enough supported images in %s' % images_dir)
                exit(1)
        else:
            # We have an images database, just load it
            photos = lib.load_images_database(images_database_file)

        log.ODM_INFO('Found %s usable images' % len(photos))

		from opendm import system 
		system.mkdir_p(os.path.join(current_path, 'opensfm'))
		# Create reconstruction object
		reconstruction = types.ODM_Reconstruction(photos)
		opensfm_interface.invent_reference_lla(images_filepath,  photo_list,os.path.join(current_path, 'opensfm'))
		
		system.mkdir_p(os.path.join(current_path,'odm_georeferencing'))
		odm_georeferencing = io.join_paths(current_path, 'odm_georeferencing')
		odm_georeferencing_coords = io.join_paths(odm_georeferencing, 'coords.txt')
		
		reconstruction.georeference_with_gps(photos, odm_georeferencing_coords, True)
		odm_geo_proj = io.join_paths(odm_georeferencing, 'proj.txt')
		reconstruction.save_proj_srs(odm_geo_proj) 
		from opendm.osfm import OSFMContext 
		octx = OSFMContext(os.path.join(current_path, 'opensfm'))
		print('----------Export geocroods--------')
		octx.run('export_geocoords --transformation --proj \'%s\'' % reconstruction.georef.proj4())
		print('----------Export Geocoords Ppppp--------')
		lib.odm_mesh_function(opensfm_config,current_path, max_concurrency, reconstruction)

		#lib.odm_mesh_function(current_path, max_concurrency)

		end = timer()
		odm_mesh_time = end - start

		start = timer()


		lib.odm_texturing_function(current_path)

		end = timer()
		odm_texturing_time = end - start

	except Exception as e:
        	# print(e.message)
        	print(traceback.print_exc())
	except KeyboardInterrupt:
        	print('keyboard interrupt')
        	sys.exit(0)
