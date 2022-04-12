import os
import io
import time
import glob
import grpc
import traceback
import shutil

from concurrent import futures
from concurrent.futures import ThreadPoolExecutor

# import faulthandler
# faulthandler.enable()


import sys
sys.path.append('../')
sys.path.append('./opensfm/opensfm/')
sys.path.append('/home/vm2/Desktop/ODM/SuperBuild/src/opensfm')
sys.path.append('/home/vm2/Desktop/ODM/SuperBuild/install/lib/python2.7/dist-packages')
sys.path.append('/home/vm2/Desktop/ODM/SuperBuild/install/lib')


import sendFile_pb2, sendFile_pb2_grpc

CHUNK_SIZE = 1024 * 1024 * 8  # 1MB

#from geopy import distance

from opendm import log
#from opendm import config
from opendm import system
from opendm import io
from opendm import gsd
from opendm import types

from collections import defaultdict

import stages

from opensfm import exif

import opensfm_interface

from opensfm_modified import tracking
from opensfm_modified import new_matching
from opensfm_modified import reconstruction

import mve_interface
import mvs_texturing
import filterpoint_interface
import mesh_interface
import export_visualsfm_helper
import compute_depthmaps_helper
import export_ply_helper

import json


from timeit import default_timer as timer


dir_path = os.path.dirname(os.path.realpath(__file__))

print('dir path: ' + str(dir_path))


def save_images_database(photos, database_file):
    with open(database_file, 'w') as f:
        f.write(json.dumps(map(lambda p: p.__dict__, photos)))
    
    log.ODM_INFO("Wrote images database: %s" % database_file)

def load_images_database(database_file):
    # Empty is used to create types.ODM_Photo class
    # instances without calling __init__
    class Empty:
        pass

    result = []

    log.ODM_INFO("Loading images database: %s" % database_file)

    with open(database_file, 'r') as f:
        photos_json = json.load(f)
        for photo_json in photos_json:
            p = Empty()
            for k in photo_json:
                setattr(p, k, photo_json[k])
            p.__class__ = types.ODM_Photo
            result.append(p)

    return result 


def get_file_chunks(filename):
    print('file name in get file chucks ' + filename)
    
    

    with open(filename, 'rb') as f:
        while True:
            piece = f.read(CHUNK_SIZE)
            if len(piece) == 0:
                return
            yield sendFile_pb2.Chunk(content=piece)
       


def save_chunks_to_file(chunks, filename):
    print('save chuck size: ' + str(CHUNK_SIZE)+ ' Bytes  to filename: ' + str(filename))
    
    #print('/'.join(filename.split('/')[0:-1]))
    system.mkdir_p('/'.join(filename.split('/')[0:-1]))
    


    with open(filename, 'wb') as f:
        for chunk in chunks:
            f.write(chunk.content)


class FileClient:
    def __init__(self, address, nodeid):
        # strongly typed for now
        self.nodeid = str(nodeid)
        print(nodeid)
        print('address' + str(address))
        channel = grpc.insecure_channel(address, options = [
	    ('grpc.max_message_length',  50 * 1024 * 1024),
            ('grpc.max_send_message_length', 50 * 1024 * 1024),
            ('grpc.max_receive_message_length', 50 * 1024 * 1024)
        ]) 
        self.stub = sendFile_pb2_grpc.FileServiceStub(channel)

    def sendTask(self, taskName, this_nodeid, taskDir, pairs=[], compute_filepath = '', submodel_path = '', cluster_images = []):


        #send Task -> task name 

        try:
            print('task ' + str(taskName) + ' id ' + str(this_nodeid))

            
           
		
            task = sendFile_pb2.Task(taskName = taskName, nodeId='node'+str(this_nodeid))
            if (taskName == 'feature_matching_pairs'):
		#print(pairs)
		#task.compute_filenames.extend(pairs)
                for each_pair in pairs:
                    n1 = task.feature_pair.add()
                    n1.pair1 = each_pair[0]
                    n1.pair2 = each_pair[1]
            if (taskName == 'create_tracks'):
                task.compute_filenames.extend(cluster_images)


                task.filename = submodel_path # name of submodel

            response = self.stub.sendTask(task)

	  
	   
            response_dir = dir_path + '/node' + str(this_nodeid) + taskDir + submodel_path
            print(' response dir ' + str(response_dir))
            if not os.path.isdir(response_dir):
                system.mkdir_p(response_dir)
            print(response_dir)

            file_chunk_list = []
            current_filename = ''
            for each in response:
                print('file name ' + str(each.filename))
                if current_filename == '': 
                    current_filename = each.filename
                if each.filename != current_filename:
                    #empty the file chucks into the file
                    print(current_filename)
                    save_chunks_to_file(file_chunk_list, response_dir + '/' + current_filename)

                    current_filename = each.filename
                    file_chunk_list = []
                    file_chunk_list.append(each)

                else:
                    file_chunk_list.append(each)
                
                #print(each.content)
            if len(file_chunk_list) > 0:
                if current_filename == 'camera_models.json':
                    response_dir = dir_path + '/node' + str(this_nodeid)
                save_chunks_to_file(file_chunk_list, response_dir + '/' + current_filename)


            print('len of file chuck ')
            print(len(file_chunk_list))
            print(current_filename)
            





            # print(str(response))
            # print(type(response))
        
            return response

        except Exception as e:
            print(' Exception : ' + str(e.message))
            print(traceback.print_exc())







    def upload(self, file_path, filename_list, submodel_file='', submodel_name = ''):
        

        #https://stackoverflow.com/questions/45071567/how-to-send-custom-header-metadata-with-python-grpc
        folder = file_path.split('/')[-1]
        print('folder ' + str(folder))

        if submodel_file=='matches':
            folder = os.path.join(folder, 'matches')
            file_path = os.path.join(file_path, 'matches')
            print(folder)
            print(filename_list)

            #test = opensfm_interface.load_matches(file_path, filename_list[1])
            
        if submodel_file=='feature':
            folder = os.path.join(submodel_name)
            #file_path = os.path.join(file_path, 'matches')

        if submodel_file=='images':
            folder = os.path.join(submodel_name, 'images')
            file_path = os.path.join(file_path, 'images')

        if submodel_file == 'exif':

            folder = os.path.join(submodel_name, 'exif')
            file_path = os.path.join(file_path, 'exif')
            newlist = []
            for i in range(len(filename_list)):
                newlist.append(filename_list[i]+'.exif')
                print(newlist[i])
            filename_list = newlist
        

            print(filename_list)
            
        if submodel_file == 'camera_models':
            folder = submodel_name


        




        for each_file in filename_list:   
                print('file')
                print(io.join_paths(file_path, each_file))
                print(each_file)
                
                chunks_generator = get_file_chunks(io.join_paths(file_path, each_file))
                print('folder here before call ' + str(folder))
                if(folder is None):
                    print('folder is none')
                #foldername = 'images'
                call_future , call = self.stub.upload.with_call(chunks_generator, metadata=(('node-id', 'node'+self.nodeid),('filename', each_file), ('folder', folder)))
                #call_future.add_done_callback(self.process_response)
                #print('here in client upload')
                #if matches == 'matches':
                    
                print(call_future)
        # else:
        #     images = glob.glob(in_file_name_or_file_path)
        #     #print(images)
        #     for each in images: 
                
        #         chunks_generator = get_file_chunks(each)
        #         filename = each.split('/')[-1]
        #         response, call = self.stub.upload.with_call(chunks_generator, metadata=(('node-id', self.nodeid),('filename', filename)))
                #print(response.length)

            # to do loop through the files 

        #assert response.length == os.path.getsize(in_file_name_or_file_path)
        return call_future

    def download(self, target_name, out_file_name):
        response = self.stub.download(sendFile_pb2.Request(name=target_name))
        save_chunks_to_file(response, out_file_name)





"""""

Server Compute Functions

"""""

def sfm_extract_metadata_list_of_images(image_path,  opensfm_config, node_file_path, image_list = []):

    try:
	
        ref_image = os.listdir(image_path)
        if (len(image_list) > 0):
            ref_image = image_list
        print('inside sfm extract')
        print(str(ref_image))
        camera_models = {}
        for i in range(len(ref_image)):
            each = ref_image[i]
            print('path ' + io.join_paths(image_path, each))
            if os.path.isfile(io.join_paths(image_path, each)):
                print('path is valid')
                d  = opensfm_interface.extract_metadata_image(io.join_paths(image_path, each), opensfm_config)
                if d['camera'] not in camera_models:
                    camera = exif.camera_from_exif_metadata(d, opensfm_config)
                    camera_models[d['camera']] = camera
                print('nodefile path ' + str(node_file_path))
                opensfm_interface.save_exif(io.join_paths(node_file_path,'exif'),each,d)
            
            else:
                print('path is not valid')
        opensfm_interface.save_camera_models(node_file_path, camera_models)
           

        return True
    except Exception as e:
        print(e.message)
        print(traceback.print_exc())
        # print(traceback.print_exception())
        
        return False
    



def sfm_detect_features(ref_image, current_path, opensfm_config, image_list = []):

    """
     feature path 
     image path
     opensfm config

    """
    print('detect feature' + str(current_path))
    if (len(image_list) > 0):
            ref_image = image_list

    feature_path = current_path + '/features'
    
    image_path = current_path + '/images/'

    try: 
        for each in ref_image:
            opensfm_interface.detect(feature_path, image_path+each,each ,opensfm_config)
    except Exception as e:
        print(traceback.print_exc())
        return False
    
    return True

def sfm_feature_matching(current_path, ref_image, cand_images , opensfm_config):

    """

    image_path
    ref_image : list of images 
    opensfm config


    """
    try: 

        # load exif is needed
        print('feature matching')

        pairs_matches, preport = new_matching.match_images(current_path+'/', ref_image, ref_image, opensfm_config)
        print(pairs_matches)
        new_matching.save_matches(current_path+'/', ref_image, pairs_matches)

        #tracking.load_matches(current_path, ref_image)
    except Exception as e:
        print(traceback.print_exc())
        return False

    return 

def sfm_feature_matching_pairs(current_path, pairs , opensfm_config):

    """

    image_path
    ref_image : list of images 
    opensfm config


    """
    try: 

        # load exif is needed
        print('feature matching')
        new_m ={}
        for each in pairs:
            ref_image = [each[0],each[1]]
            pairs_matches, preport = new_matching.match_images(current_path+'/', ref_image, ref_image, opensfm_config)
            new_m.update(pairs_matches)
            #print(pairs_matches)
            #new_matching.save_matches(current_path+'/', ref_image, pairs_matches)
            #tracking.load_matches(current_path, ref_image
            return new_m
    except Exception as e:
        print(traceback.print_exc())
        return None

    return 

def sfm_create_tracks(current_path, ref_image, opensfm_config, matches = {}, self_compute=False, self_path=''):
    """
    
    path to features 
    ref_image list 
    opensfm config


    """

    try: 
        print('sfm create tracks: ' + str(current_path))

        if self_compute:
            features, colors = tracking.load_features(self_path+'/features', ref_image, opensfm_config)
            if bool(matches) is False:
                matches = tracking.load_matches(self_path, ref_image)
        

            print('feature length: ' + str(len(features)))

            graph = tracking.create_tracks_graph(features, colors, matches,
                                                opensfm_config)


            opensfm_interface.save_tracks_graph(graph, current_path)


        else:

            features, colors = tracking.load_features(current_path+'/features', ref_image, opensfm_config)
            if bool(matches) is False:
                matches = tracking.load_matches(current_path, ref_image)
        

            print('feature length: ' + str(len(features)))

            graph = tracking.create_tracks_graph(features, colors, matches,
                                                opensfm_config)


            opensfm_interface.save_tracks_graph(graph, current_path)
    
    except Exception as e:
        print(traceback.print_exc())
        return False
    
    return True


def sfm_opensfm_reconstruction(current_path, opensfm_config, self_compute=False, self_path=''):


    try:


        graph = opensfm_interface.load_tracks_graph(current_path)

        if self_compute:
            report, reconstructions = reconstruction.incremental_reconstruction(self_path, graph, opensfm_config)
            opensfm_interface.save_reconstruction(current_path,reconstructions)

        else:

            report, reconstructions = reconstruction.incremental_reconstruction(current_path, graph, opensfm_config)
            opensfm_interface.save_reconstruction(current_path,reconstructions)
    except Exception as e:
        print(traceback.print_exc())
        return False
    return True 

def sfm_max_undistort_image_size(current_path, image_path, self_compute = False):
    outputs = {}
    photos = []
    from opendm import photo
    from opendm import types

    ref_image = os.listdir(image_path)

    # get the 
    
    try: 
        for each in ref_image:
            photos += [types.ODM_Photo(os.path.join(image_path, each))]
        # get match image sizes
        if self_compute:
            outputs['undist_image_max_size'] = max(
		    gsd.image_max_size(photos, 5.0, os.path.join(current_path ,'reconstruction.json')),
		    0.1)
	else:
            outputs['undist_image_max_size'] = max(
		    gsd.image_max_size(photos, 5.0, os.path.join(current_path, 'submodel_0' ,'reconstruction.json')),
		    0.1)        
            # print(outputs)

            # #undistort image dataset: 

            #
    except Exception as e:
	print(current_path)
	outputs['undist_image_max_size'] = max(
		    gsd.image_max_size(photos, 5.0, os.path.join(current_path ,'reconstruction.json')),
		    0.1
		)   
                        
        print(traceback.print_exc())
        return outputs['undist_image_max_size'] 

    return outputs['undist_image_max_size'] 


def sfm_undistort_image(current_path, opensfm_config, self_compute=False, self_path=''):
    
    try:
        import opensfm_undistort_command

        if self_compute:
            opensfm_undistort_command.opensfm_undistort(current_path, opensfm_config, self_compute, self_path)

        else:
            opensfm_undistort_command.opensfm_undistort(current_path, opensfm_config)
    except Exception as e:
        print(traceback.print_exc())
        return False
    return True

def sfm_export_visual_sfm(current_path, opensfm_config, self_compute=False, self_path=''):


    try:
        export_visualsfm_helper.open_export_visualsfm(current_path, opensfm_config)
    except Exception as e:
        print(traceback.print_exc())
        return False

    return 


def sfm_compute_depthmaps(current_path, opensfm_config, self_compute=False, self_path=''):

    try:
        if self_compute:
            compute_depthmaps_helper.open_compute_depthmaps(current_path, opensfm_config, self_compute, self_path)
        else:
            compute_depthmaps_helper.open_compute_depthmaps(current_path, opensfm_config)
    except Exception as e:
        print(traceback.print_exc())
        return False
    
    return True 


def mve_dense_reconstruction(current_path, max_concurrency, self_compute=False, self_path =''):

    try:

        mve_file_path = os.path.join(current_path,'mve')

        max_image = None
        if self_compute:
            max_image = sfm_max_undistort_image_size(self_path, os.path.join(self_path, 'images'))
        else:
            max_image = sfm_max_undistort_image_size(current_path, os.path.join(current_path, 'images'))

        mve_interface.mve_dense_recon(max_image, mve_file_path, max_concurrency)
    except Exception as e:
        print(traceback.print_exc())
        return False
    
    return True


def mve_makescene_function(current_path, max_concurrency, self_compute=False, self_path=''):

    try: 
        mve_file_path =os.path.join(current_path,'mve')
        if os.path.isdir(mve_file_path):
            shutil.rmtree(mve_file_path)
        print(mve_file_path)
        nvm_file = os.path.join(current_path,'undistorted', 'reconstruction.nvm')
        print(nvm_file)
        print('make scene function')
        mve_interface.mve_makescene(nvm_file, mve_file_path, max_concurrency)
    except Exception as e:
        print(traceback.print_exc())
        return False

    return True 


def mve_scene2pset_function(current_path, max_concurrency, self_compute=False, self_path='' ):

    try:
        mve_file_path = os.path.join(current_path,'mve')
        mve_model = io.join_paths(mve_file_path, 'mve_dense_point_cloud.ply')
        max_image = None
        if self_compute:
            max_image = sfm_max_undistort_image_size(self_path, os.path.join(self_path, 'images'))
        else:
            max_image = sfm_max_undistort_image_size(current_path, os.path.join(current_path, 'images'))

        mve_interface.mve_scene2pset(mve_file_path, mve_model,max_image,max_concurrency)
    except Exception as e:
        print(traceback.print_exc())
        return False

    return True 

def mve_cleanmesh_function(current_path, max_concurrency):
    
    try:
        mve_file_path = os.path.join(current_path,'mve')
        mve_model = io.join_paths(mve_file_path, 'mve_dense_point_cloud.ply')
        mve_interface.mve_cleanmesh(0.6, mve_model, max_concurrency)
    except Exception as e:
        print(traceback.print_exc())
        return False
    return True


def odm_filterpoints_function(current_path, max_concurrency):

    try:
        mve_file_path = os.path.join(current_path,'mve')
        mve_model = io.join_paths(mve_file_path, 'mve_dense_point_cloud.ply')

        odm_filterpoints = os.path.join(current_path,'filterpoints')
        filterpoint_cloud = io.join_paths(odm_filterpoints, "point_cloud.ply")

        filterpoint_interface.filter_points(odm_filterpoints, mve_model, filterpoint_cloud, max_concurrency)
    except Exception as e:
        print(traceback.print_exc())
        return False
    return True

def odm_mesh_function(opensfm_config, current_path, max_concurrency, reconstruction):


    try:
    	opensfm_config['use_3dmesh']=False
        odm_filterpoints = os.path.join(current_path,'filterpoints')
        filterpoint_cloud = io.join_paths(odm_filterpoints, "point_cloud.ply")

        odm_mesh_folder= os.path.join(current_path,'mesh')
        if not os.path.isdir(odm_mesh_folder):
            os.mkdir(odm_mesh_folder)

        #odm_mesh_ply = io.join_paths(odm_mesh_folder, "odm_mesh.ply")
	odm_mesh_ply = io.join_paths(odm_mesh_folder, "odm_mesh.ply")
        mesh_interface.mesh_3d(opensfm_config,odm_mesh_folder, odm_mesh_ply, filterpoint_cloud, max_concurrency, reconstruction, current_path)
    except Exception as e:
        print(traceback.print_exc())
        return False

    return True

def odm_texturing_function(current_path, submodel=False):
      
    try:   
        mvs_folder= os.path.join(current_path,'mvs')
        odm_mesh_folder= os.path.join(current_path,'mesh')
        odm_mesh_ply = io.join_paths(odm_mesh_folder, "odm_mesh.ply")
       


        
        nvm_file = os.path.join(current_path,'undistorted', 'reconstruction.nvm')

        mvs_texturing.mvs_texturing(odm_mesh_ply, mvs_folder, nvm_file)  
    except Exception as e:
        print(traceback.print_exc())
        return False

    return True

def export_ply_function(current_path, opensfm_config, self_compute=False, self_path=''):

    try:
        if self_compute:
            export_ply_helper.open_export_ply(current_path, opensfm_config, self_compute, self_path)
        else:
            export_ply_helper.open_export_ply(current_path, opensfm_config)
    except Exception as e:
        print(traceback.print_exc())
        return False
    
    return True 



def write_json(data, filename):
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)

def write_json_append(data, filename):
    if not os.path.isfile(filename):
        write_json(data, filename)
    else:
        with open(filename) as f:
            old_data = {}
            try:
                old_data = json.load(f)
            except Exception as e:
                print(e.message)
                pass
            except:
                pass
            data.update(old_data)

        write_json(data, filename)  







class FileServer(sendFile_pb2_grpc.FileServiceServicer):
    def __init__(self, port, node_id, dataset_path):
        self.port = port
        self.node_id = str(node_id)
        node_path = io.join_paths(dir_path, 'node'+self.node_id)
        self.dataset_dir = dataset_path
        #configuration
        # args  = config.config()
        # args.project_path = "./dataset/images"
        self.datapath = 'dataset'
        # data = ODMLoadDatasetStage(self.datapath , args, progress=5.0,
        #                                   verbose=args.verbose)

        #distance to include the image in meters
        self.include_distance = 10

        #run the dataset layer
        #data.run()

        print('sfm')
        print(self.datapath)

        #opensfm configuration

        opensfm_config = opensfm_interface.setup_opensfm_config(self.datapath)
        self.opensfm_config = opensfm_config

        #extract metadata 

        # camera_models = {}
        # current_path = '/home/j/ODM-master/grpc_stages/node1/'

        # ref_image = ['DJI_0019.JPG', 'DJI_0018.JPG','DJI_0020.JPG','DJI_0021.JPG','DJI_0022.JPG','DJI_0023.JPG','DJI_0024.JPG'
        # ,'DJI_0025.JPG','DJI_0026.JPG','DJI_0027.JPG','DJI_0028.JPG','DJI_0029.JPG','DJI_0030.JPG','DJI_0031.JPG','DJI_0032.JPG','DJI_0033.JPG','DJI_0034.JPG','DJI_0035.JPG']
        # cand_images = ['DJI_0019.JPG', 'DJI_0018.JPG','DJI_0020.JPG','DJI_0021.JPG','DJI_0022.JPG','DJI_0023.JPG','DJI_0024.JPG'
        # ,'DJI_0025.JPG','DJI_0026.JPG','DJI_0027.JPG','DJI_0028.JPG','DJI_0029.JPG','DJI_0030.JPG','DJI_0031.JPG','DJI_0032.JPG','DJI_0033.JPG','DJI_0034.JPG','DJI_0035.JPG']

        # for each in ref_image:
        #     d  = opensfm_interface.extract_metadata_image('/home/j/ODM-master/grpc_stages/node1/'+each, opensfm_config)
        #     if d['camera'] not in camera_models:
        #         camera = exif.camera_from_exif_metadata(d, opensfm_config)
        #         camera_models[d['camera']] = camera 
             #opensfm_interface.save_exif('/home/j/ODM-master/grpc_stages/node1/exif', each,d)
         #opensfm_interface.save_camera_models('/home/j/ODM-master/grpc_stages/node1/', camera_models)
        
        # #c  = opensfm_interface.extract_metadata_image('/home/j/ODM-master/grpc_stages/node1/DJI_0019.JPG', opensfm_config)
       

        # print(d)
        # print(camera_models)
        # #opensfm_interface.save_exif('/home/j/ODM-master/grpc_stages/node1/', 'DJI_0019.JPG', c)
        
        # #save the exif metadata to file in a folder
        # # send extracted metedata and camera model back

 

        # #feature extraction
        # for each in ref_image:
        #     opensfm_interface.detect(current_path+'features', current_path+each,each ,opensfm_config)


        # #opensfm_interface.detect(current_path+'features', current_path+'DJI_0019.JPG','DJI_0019.JPG' ,opensfm_config)




        # #feature matching

       

        # pairs_matches, preport = new_matching.match_images(current_path, ref_image, cand_images, opensfm_config)
        # new_matching.save_matches(current_path, ref_image, pairs_matches)
        # print('matching')
        



        # #create tracks first

        # features, colors = tracking.load_features(current_path+'features', ref_image, opensfm_config)
        # matches = tracking.load_matches(current_path, ref_image)
        # graph = tracking.create_tracks_graph(features, colors, matches,
        #                                      opensfm_config)

        # opensfm_interface.save_tracks_graph(graph, current_path)



        # #reconstruction


        # # load tracks graph

        # graph = opensfm_interface.load_tracks_graph(current_path)
        # report, reconstructions = reconstruction.incremental_reconstruction(current_path, graph, opensfm_config)

        # opensfm_interface.save_reconstruction(current_path,reconstructions)
        # #opensfm_interface.save_report(io.json_dumps(report), 'reconstruction.json')
      
        # outputs = {}
        # photos = []
        # from opendm import photo
        # from opendm import types
        
        # for each in ref_image:
        #     photos += [types.ODM_Photo(current_path+each)]
          
        
        # # get match image sizes
        # outputs['undist_image_max_size'] = max(
        #     gsd.image_max_size(photos, 5.0, current_path+'reconstruction.json'),
        #     0.1
        # )        
        # print(outputs)

        # #undistort image dataset: 

        # opensfm_interface.opensfm_undistort(current_path, opensfm_config)


        # #export visualsfm

        # opensfm_interface.open_export_visualsfm(current_path, opensfm_config)

        # #compute depthmaps 

        # opensfm_interface.open_compute_depthmaps(current_path, opensfm_config)

      
        # #mve stage 1 makescene

        # #input compute depthmaps file
        
        # mve_file_path = '/home/j/ODM-master/grpc_stages/node1/mve'
        # nvm_file = '/home/j/ODM-master/grpc_stages/node1/undistorted/reconstruction.nvm'
        # mve_interface.mve_makescene(nvm_file, mve_file_path, 2)


        # #mve stage 2 dense reconstruction

        # mve_interface.mve_dense_recon(outputs['undist_image_max_size'], mve_file_path, 2)

        # #mve stage 3 scene2pset_path
        # mve_model = io.join_paths(mve_file_path, 'mve_dense_point_cloud.ply')
        # mve_interface.mve_scene2pset(mve_file_path, mve_model,outputs['undist_image_max_size'],2)

        # #mve stage 4 clean_mesh
        # mve_interface.mve_cleanmesh(0.6, mve_model, 2)



        # # filterpoint cloud
        # odm_filterpoints = '/home/j/ODM-master/grpc_stages/node1/filterpoints'
        # filterpoint_cloud = io.join_paths(odm_filterpoints, "point_cloud.ply")

        # filterpoint_interface.filter_points(odm_filterpoints, mve_model, filterpoint_cloud,2)

        #meshing stage
        # odm_mesh_folder= '/home/j/ODM-master/grpc_stages/node1/mesh'
        # odm_mesh_ply = io.join_paths(odm_mesh_folder, "odm_mesh.ply")
        # mesh_interface.mesh_3d(odm_mesh_folder, odm_mesh_ply, filterpoint_cloud, 2)

        # #texturing stage

        # mvs_folder= '/home/j/ODM-master/grpc_stages/node1/mvs'
        # mvs_texturing.mvs_texturing(odm_mesh_ply, mvs_folder, nvm_file)       

        #https://stackoverflow.com/questions/45071567/how-to-send-custom-header-metadata-with-python-grpc


        class Servicer(sendFile_pb2_grpc.FileServiceServicer):
            def __init__(self):
                self.tmp_file_name = './dataset2/IMG_2359.JPG'


            def upload(self, request_iterator, context):
                # client uploads images to this node
                #request iterator is the file iterator through the chuncks

                #self.tmp_file_name is the name to save the file chucks to 
                nodeid = ''
                filename = ''
                folder = ''
                for key, value in context.invocation_metadata():
                    
                    if(key == 'node-id'):
                        nodeid = value
                        #check if there is a dir for the node id
                        system.mkdir_p('node'+str(node_id)+'/'+nodeid)
                    if key == 'filename':
                        filename = value
                    if key == 'folder':
                        folder = value
                        print('node_id '+ str(node_id))
                        system.mkdir_p('node'+str(node_id)+'/'+nodeid+'/'+folder)

                 
                    print('Received initial metadata: key=%s value=%s' % (key, value))
                #print(os.path.dirname(os.path.abspath(__file__)))
                if (nodeid != '' and filename  != ''):
                    save_chunks_to_file(request_iterator, os.path.join('node'+str(node_id),nodeid, folder, filename))

                    return sendFile_pb2.UploadStatus(message = " Successul ", code=sendFile_pb2.UploadStatusCode.Ok) 
                else:
                    print('bad node id and bad filename')
                    # reply = sendFile_pb2.UploadStatus()
                    # reply.Message = " Failed "
                    # reply.c
                    return sendFile_pb2.UploadStatus(message = " Failure ", code=sendFile_pb2.UploadStatusCode.Failed)

            def download(self, request, context):
                if request.name:
                    return get_file_chunks(self.tmp_file_name)

            def sendTask(self, request, context):
                taskName = request.taskName
                nodeid = request.nodeId
                print('here in send Task ' + str(taskName) + ' ' + str(nodeid))

                #node_path is the path of this node
                print(node_path)
                req_node_path = io.join_paths(node_path, str(nodeid))
                print(req_node_path)
                try:
                    
                    start_timer = timer()
                    req_node_path_timer = os.path.join(req_node_path, 'compute_times.json')


                    if(taskName == 'exif'):
                        # check 
                            start = timer()
                            print('run sfm extract metadata')
                           
                            image_path =   req_node_path + '/images'
                            print('image path ' + str(image_path))
                            is_complete = sfm_extract_metadata_list_of_images(image_path,opensfm_config, req_node_path)
                            
                            if (is_complete):

                                #write time into json file timer
                                end_timer = timer()-start
                                write_json({req_node_path+'exif': end_timer}, os.path.join(req_node_path, 'exif.json'))

                                
                                print('is complete')
                                #files to send back to the main node
                                # #camera model.json and <nodeid>/ exif folder
                                exif_folder_path = req_node_path + '/exif'
                                print(exif_folder_path)
                                
                            
                                exif_list =  os.listdir(exif_folder_path)
                                print(exif_list)
                                exif_list.append('camera_models.json')
				
			        

                                for each_file in exif_list:
                                    folder_path = exif_folder_path
                                    if each_file == 'camera_models.json':
                                        folder_path = req_node_path

            


                                    
                                    with open(io.join_paths(folder_path, each_file), 'rb') as f:
                                        while True:
                                            piece = f.read(CHUNK_SIZE)
                                            if len(piece) == 0:
                                                break
                                            yield sendFile_pb2.NewChunk(filename=each_file  ,content=piece)

                                #send camera model.json

                               

                                shutil.rmtree(exif_folder_path)
                                os.mkdir(exif_folder_path)





                                return
                            else: 
                                print('not complete')
                    elif(taskName == 'detect_features'):

                        print('detect features')
			start = timer()
                       
                   
                        image_path = req_node_path + '/images'
                        print('image path ' + str(image_path))
                        image_list = os.listdir(image_path)

                        is_complete =sfm_detect_features(image_list,req_node_path ,opensfm_config)



                        if(is_complete):

                            #write time into json file timer
                            end_timer = timer()-start
                            write_json({req_node_path+'detect_features': end_timer},"features.json")

                         

                            detect_folder_path = req_node_path + '/features'
                            print(detect_folder_path)


                            _list =  os.listdir(detect_folder_path)
                            print(_list)

                            for each_file in _list:
                                
                                with open(io.join_paths(detect_folder_path, each_file), 'rb') as f:
                                    while True:
                                        piece = f.read(CHUNK_SIZE)
                                        if len(piece) == 0:
                                            break
                                        yield sendFile_pb2.NewChunk(filename=each_file  ,content=piece)



                            #remove folder
                            shutil.rmtree(detect_folder_path)
                              


                        return





                    elif(taskName == 'feature_matching'):

                   
                        image_path = req_node_path + '/images'
                        print('image path ' + str(image_path))
                        image_list = os.listdir(image_path)
                        detect_folder_path = req_node_path + '/features'


                        is_complete = sfm_feature_matching(req_node_path, image_list, image_list, opensfm_config)
                        

                        if(is_complete):

                            
                            print(detect_folder_path)


                            _list =  os.listdir(detect_folder_path)
                            print(_list)

                            for each_file in _list:
                                
                                with open(io.join_paths(detect_folder_path, each_file), 'rb') as f:
                                    while True:
                                        piece = f.read(CHUNK_SIZE)
                                        if len(piece) == 0:
                                            break
                                        yield sendFile_pb2.NewChunk(filename=each_file  ,content=piece)

                            shutil.rmtree(detect_folder_path)
                        #write time into json file timer
                        end_timer = timer()-start_timer
                        write_json_append({req_node_path+'feature_matching': end_timer}, req_node_path_timer)


                        return
                    elif(taskName == 'feature_matching_pairs'):
			start = timer()
                        image_path =   req_node_path + '/images'
                        
                        print('image path ' + str(image_path))
                        #image_list =os.listdir(image_path)
                        #pair1 = request.feature_pair.pair1
                        #pair2 = request.feature_pair.pair2

                        

                        #print('pairs ' + str(pair1) + ' ' + str(pair2))

			lis = list(request.feature_pair)
			pairs_matches = {}
			for each in lis:
                        	pairs_m = sfm_feature_matching_pairs(req_node_path,[(each.pair1, each.pair2)] ,opensfm_config)
				pairs_matches.update(pairs_m)
                       

                        print('here in main taskName')
                        print(pairs_matches)
			print(len(pairs_matches))
                        filename = str(node_id)


                        # save pair matches in a file 
                        # send it to client

                        opensfm_interface.save_matches(req_node_path, filename, pairs_matches)

                        #write time into json file timer
                        end_timer = timer()-start
                        write_json({'_feature_matching_pairs' + filename: end_timer}, filename+'.json')

                        #for key in pairs_matches:
			#	print(key)
			#print(len(pairs_matches))

                        detect_folder_path = req_node_path + '/matches'
                        with open(io.join_paths(detect_folder_path, filename+'_matches.pkl.gz'), 'rb') as f:
                                    while True:
                                        piece = f.read(CHUNK_SIZE)
                                        if len(piece) == 0:
                                            break
                                        yield sendFile_pb2.NewChunk(filename=filename+'_matches.pkl.gz'  ,content=piece)
                       
                        #os.remove(io.join_paths(detect_folder_path, filename+'_matches.pkl.gz'))

                        return 

                        


                       


                        
                    
                    elif(taskName == 'create_tracks'):

                        print('create_tracks here in function 212')

                        timer_map = {}

                        start = timer()
                        # ex. submodel 1 , submodel 2, submodel 3
                        compute_filename = request.filename

                        print(compute_filename)

                        # image file names
                        compute_images = request.compute_filenames


                        print(compute_images)

                        submodel_path = os.path.join(req_node_path ,compute_filename)

                        print(submodel_path)
		

                        submodel_path_match = os.path.join(submodel_path, 'matches')

                        submodel_matches = opensfm_interface.load_matches(submodel_path_match, compute_filename, True)

                        sfm_create_tracks(submodel_path, compute_images, opensfm_config, submodel_matches)


                        end = timer()
                        sfm_create_tracks_timer = end - start

                        start = timer()
                        
                        sfm_opensfm_reconstruction(submodel_path, opensfm_config)


                        end = timer()
                        sfm_opensfm_reconstruction_time = end - start

                        start = timer()

                        sfm_undistort_image(submodel_path, opensfm_config)

                        end = timer()
                        sfm_undistort_image_time = end - start


                        start = timer()
                        
                        sfm_export_visual_sfm(submodel_path, opensfm_config)


                        end = timer()
                        sfm_export_visualsfm_time = end - start


                        start = timer()

                        #sfm_compute_depthmaps(submodel_path, opensfm_config)
			export_ply_function(submodel_path, opensfm_config)

        		end = timer()
        		sfm_export_ply_time = end - start



                        start = timer()

                        max_concurrency = 4

                        # delete from makescene

                        mve_makescene_function(submodel_path, max_concurrency)



                        end = timer()
                        mve_makescene_function_time = end - start

                        start = timer()

                        mve_dense_reconstruction(submodel_path, max_concurrency)

                        end = timer()
                        mve_dense_reconstruction_time = end - start

                        start = timer()

                        mve_scene2pset_function(submodel_path, max_concurrency)

                        end = timer()
                        mve_mve_scene2pset_time = end - start

                        start = timer()

                        mve_cleanmesh_function(submodel_path,max_concurrency)

                        end = timer()
                        mve_mve_cleanmesh_time = end - start


                        start = timer()


                        odm_filterpoints_function(submodel_path, max_concurrency)


                        end = timer()
                        odm_filterpoint_time = end - start
                        
                        start = timer()
			
			images_database_file = io.join_paths(submodel_path, 'images.json')
			photo_list =  os.listdir(os.path.join(submodel_path, 'images'))
	
			photos = []
			images_dir = io.join_paths(submodel_path,'images')
			if not io.file_exists(images_database_file):
			    files = photo_list
			    
			    if files:
				# create ODMPhoto list
				path_files = [io.join_paths(images_dir, f) for f in files]

				
				dataset_list = io.join_paths(submodel_path,'img_list')
				with open(dataset_list, 'w') as dataset_list:
				    log.ODM_INFO("Loading %s images" % len(path_files))
				    for f in path_files:
				        photos += [types.ODM_Photo(f)]
				        dataset_list.write(photos[-1].filename + '\n')

				# Save image database for faster restart
				save_images_database(photos, images_database_file)
			    else:
				log.ODM_ERROR('Not enough supported images in %s' % images_dir)
				exit(1)
			else:
			    # We have an images database, just load it
			    photos = load_images_database(images_database_file)

			log.ODM_INFO('Found %s usable images' % len(photos))

			# Create reconstruction object
			reconstruction = types.ODM_Reconstruction(photos) 
			log.ODM_INFO('Found %s usable images' % len(photos))
			from opendm import system 
			system.mkdir_p(os.path.join(submodel_path, 'opensfm'))
			# Create reconstruction object
			reconstruction = types.ODM_Reconstruction(photos)
			opensfm_interface.invent_reference_lla(submodel_path,photo_list ,os.path.join(submodel_path, 'opensfm'))
	
			system.mkdir_p(os.path.join(submodel_path,'odm_georeferencing'))
			odm_georeferencing = io.join_paths(submodel_path, 'odm_georeferencing')
			odm_georeferencing_coords = io.join_paths(odm_georeferencing, 'coords.txt')
	
			reconstruction.georeference_with_gps(photos, odm_georeferencing_coords, True)
			odm_geo_proj = io.join_paths(odm_georeferencing, 'proj.txt')
			reconstruction.save_proj_srs(odm_geo_proj) 
			from opendm.osfm import OSFMContext 
			octx = OSFMContext(os.path.join(submodel_path, 'opensfm'))
			print('----------Export geocroods--------')
			octx.run('export_geocoords --transformation --proj \'%s\'' % reconstruction.georef.proj4())
			print('----------Export Geocoords Ppppp--------')


			odm_mesh_function(opensfm_config,submodel_path, max_concurrency, reconstruction)




                        #odm_mesh_function(submodel_path,max_concurrency)

                        end = timer()
                        odm_mesh_time = end - start
                        start = timer()


                        odm_texturing_function(submodel_path,True)

                        end = timer()
                        odm_texturing_time = end - start

			import orthophoto
			start = timer()
			import odm_georeferencing
	
	
			import config
			opendm_config = config.config()
			tree = {}
			odm_georeferencing.process(opendm_config, tree, reconstruction, submodel_path)
			odm_georeferencing = io.join_paths(submodel_path, 'odm_georeferencing')
			odm_georeferencing_coords = io.join_paths(odm_georeferencing, 'coords.txt')
			reconstruction.georeference_with_gps(photos, odm_georeferencing_coords, True)

			orthophoto.process(opendm_config,submodel_path, 4, reconstruction)
			end = timer()
                        odm_orthophoto_time = end - start

                        nodeid = str(nodeid)
                        timer_map['sfm_create_tracks_time-'+nodeid] = sfm_create_tracks_timer
                        timer_map['sfm_open_reconstruction-'+nodeid] = sfm_opensfm_reconstruction_time
                        timer_map['sfm_undistort-'+nodeid] = sfm_undistort_image_time
                        timer_map['sfm_export_visualsfm-'+nodeid] = sfm_export_visualsfm_time
                        timer_map['sfm_export_ply-'+nodeid] = sfm_export_ply_time
                        timer_map['mve_makescence_time-'+nodeid] = mve_makescene_function_time
                        timer_map['mve_dense_recon_time-'+nodeid] = mve_dense_reconstruction_time
                        timer_map['mve_scence2pset_time-'+nodeid] = mve_mve_scene2pset_time
                        timer_map['mve_cleanmesh_time-'+nodeid] =  mve_mve_cleanmesh_time
                        timer_map['odm_filerpoints-'+nodeid] =  odm_filterpoint_time
                        timer_map['odm_mesh-'+nodeid] =  odm_mesh_time
                        timer_map['odm_texturing-'+nodeid] =  odm_texturing_time


                        #write time into json file timer
                        write_json(timer_map, req_node_path_timer)

                        print('Create Tracks Total Time: ' + str(sfm_create_tracks_timer))
                        print('OpenSfm Reconstruction Total Time: ' + str(sfm_opensfm_reconstruction_time))
                        print('OpenSfm Undistort Image Total Time: ' + str(sfm_undistort_image_time))
                        print('OpenSfm Export Visual Sfm Total Time: ' + str(sfm_export_visualsfm_time))
                        print('OpenSfm Export Ply Sfm Total Time: ' + str(sfm_export_ply_time))
                        print('Mve Makescene Sfm Total Time: ' + str(mve_makescene_function_time))
                        print('Mve Dense Reconstruction Sfm Total Time: ' + str(mve_dense_reconstruction_time))
                        print('Mve Scene 2 Pset Sfm Total Time: ' + str(mve_mve_scene2pset_time))
                        print('Mve Cleanmesh Sfm Total Time: ' + str(mve_mve_cleanmesh_time))
                        print('ODM Filterpoints Total Time: ' + str(odm_filterpoint_time))
                        print('ODM Mesh Total Time: ' + str(odm_mesh_time))
                        print('ODM Texturing Total Time: ' + str(odm_texturing_time))
			print('ODM Orthophoto Total Time: ' + str(odm_orthophoto_time))
			total_time = sfm_create_tracks_timer + sfm_opensfm_reconstruction_time + sfm_undistort_image_time + sfm_export_visualsfm_time + sfm_export_ply_time + mve_makescene_function_time + mve_dense_reconstruction_time +  mve_mve_scene2pset_time + mve_mve_cleanmesh_time + odm_filterpoint_time + odm_texturing_time + odm_orthophoto_time
		        print(total_time)

                        

                        # send mesh ply
                        # send mesh dirty ply
	               

                        mesh_folder_path = submodel_path+ '/mesh'
                        files = os.listdir(mesh_folder_path)
			orthophoto_folder = os.path.join(submodel_path, 'orthophoto')
                        more_files = os.listdir(os.path.join(submodel_path, 'mvs'))	
                        files = files+more_files
			more_files = os.listdir(os.path.join(submodel_path, 'orthophoto'))	
			files = files+more_files
			mvs_folder = os.path.join(submodel_path, 'mvs')

                        for each in files:
			    if (os.path.isfile(io.join_paths(mesh_folder_path, each))):
				
		                    with open(io.join_paths(mesh_folder_path, each), 'rb') as f:
		                                while True:
		                                    piece = f.read(CHUNK_SIZE)
		                                    if len(piece) == 0:
		                                        break
		                                    yield sendFile_pb2.NewChunk(filename=each  ,content=piece)
			    elif (os.path.isfile(os.path.join(mvs_folder, each))):
					with open(io.join_paths(mvs_folder, each), 'rb') as f:
		                                while True:
		                                    piece = f.read(CHUNK_SIZE)
		                                    if len(piece) == 0:
		                                        break
		                                    yield sendFile_pb2.NewChunk(filename=each  ,content=piece)
 			    elif (os.path.isfile(os.path.join(orthophoto_folder, each))):
					with open(io.join_paths(orthophoto_folder, each), 'rb') as f:
		                                while True:
		                                    piece = f.read(CHUNK_SIZE)
		                                    if len(piece) == 0:
		                                        break
		                                    yield sendFile_pb2.NewChunk(filename=each  ,content=piece)
			    else:
				continue


                        print('after')

                        #compute everything after tracks
                        #print('create_tracks')
                        #image_path =   req_node_path + '/images'
                        #print('image path ' + str(image_path))
                        #image_list = os.listdir(image_path)

                        #sfm_create_tracks(req_node_path, image_list, opensfm_config)

                        return
                    
                    
                    elif(taskName == 'opensfm_reconstruction'):

                        sfm_opensfm_reconstruction(req_node_path, opensfm_config)
                        
                        return 
                   
                    elif(taskName == 'undistort_image'):

                        sfm_undistort_image(req_node_path, opensfm_config)

                        return 
                    elif(taskName == 'export_visualsfm'):

                        sfm_export_visual_sfm(req_node_path, opensfm_config)
                        return
                    elif(taskName == 'compute_depthmaps'):
                        sfm_compute_depthmaps(req_node_path, opensfm_config)
                        return
                    elif(taskName == 'dense_reconstruction'):
                        mve_dense_reconstruction(req_node_path)
                        return
                    elif(taskName == 'makescene_function'):
                        mve_makescene_function(req_node_path, 2)
                        return
                    elif(taskName == 'scene2pset'):
                        mve_scene2pset_function(req_node_path, 2)
                        return
                    elif(taskName == 'cleanmesh'):
                        mve_cleanmesh_function(req_node_path, 2)
                        return
                    elif(taskName == 'odm_filterpoints'):
                        odm_filterpoints_function(req_node_path,2)
                        return
                    elif(taskName == 'odm_mesh'):
                        odm_mesh_function(req_node_path, 2)
                        return
                    elif(taskName == 'odm_texturing'):
                        odm_texturing_function(req_node_path)
                        return
                    
                    
    

                except Exception as e:
                        print(e.message)
                        print(traceback.print_exc())

                        return

                            

                    









                #metadata use image name 
                #use node id 

                #opensfm compute

                # detect feature

                # 


                # if(taskName == "compute_image_feature"):

                # elif(taskName == 'compute_matching_two_images'):0
               


                # elif(taskName == 'compute_'):

                # elif(taskName == ''):

                


                

        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=4), options = [
            ('grpc.max_send_message_length', 50 * 1024 * 1024),
            ('grpc.max_receive_message_length', 50 * 1024 * 1024)
        ])

        #self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        sendFile_pb2_grpc.add_FileServiceServicer_to_server(Servicer(), self.server)


        neighbor_ip = ['50001', '50002']
        self.has_neighbor = defaultdict(lambda: "Node not present.")

        # tuple (false as any response from neighbor, filelocation)
        neighbor_response = 0 #increment neighbors response as each neighbor respond
        for each in neighbor_ip:
            self.has_neighbor[each] = (False, "")
        
        self.leader = True

        # if(self.leader):
        #     while(neighbor_response != len(neighbor_ip)):
                # wait for all the neighbors to respond 

            #finish waiting 

            #send compute task to each node

            #send images to all nodes
        
        


            

        # port 50001
        # port 50002

    def start(self, port):
        print(port)
        self.server.add_insecure_port('[::]:5001')
        self.server.start()

        print("end of init")

        try:
            while True:
                time.sleep(60*60*24)
        except KeyboardInterrupt:
            self.server.stop(0)
