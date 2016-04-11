/*
################################################################################
#  THIS FILE IS 100% GENERATED BY ZPROJECT; DO NOT EDIT EXCEPT EXPERIMENTALLY  #
#  Please refer to the README for information about making permanent changes.  #
################################################################################
*/
#ifndef Q_ZDIR_H
#define Q_ZDIR_H

#include "qczmq.h"

class QT_CZMQ_EXPORT QZdir : public QObject
{
    Q_OBJECT
public:

   //  Copy-construct to return the proper wrapped c types
   QZdir (zdir_t *self, QObject *qObjParent = 0);

    //  Create a new directory item that loads in the full tree of the specified
    //  path, optionally located under some parent path. If parent is "-", then 
    //  loads only the top-level directory, and does not use parent as a path.  
    explicit QZdir (const QString &path, const QString &parent, QObject *qObjParent = 0);

    //  Destroy a directory tree and all children it contains.
    ~QZdir ();

    //  Return directory path
    const QString path ();

    //  Return last modification time for directory.
    time_t modified ();

    //  Return total hierarchy size, in bytes of data contained in all files
    //  in the directory tree.                                              
    off_t cursize ();

    //  Return directory count
    size_t count ();

    //  Returns a sorted list of zfile objects; Each entry in the list is a pointer
    //  to a zfile_t item already allocated in the zdir tree. Do not destroy the   
    //  original zdir tree until you are done with this list.                      
    QZlist * list ();

    //  Remove directory, optionally including all files that it contains, at  
    //  all levels. If force is false, will only remove the directory if empty.
    //  If force is true, will remove all files and all subdirectories.        
    void remove (bool force);

    //  Calculate differences between two versions of a directory tree.    
    //  Returns a list of zdir_patch_t patches. Either older or newer may  
    //  be null, indicating the directory is empty/absent. If alias is set,
    //  generates virtual filename (minus path, plus alias).               
    static QZlist * diff (QZdir *older, QZdir *newer, const QString &alias);

    //  Return full contents of directory as a zdir_patch list.
    QZlist * resync (const QString &alias);

    //  Load directory cache; returns a hash table containing the SHA-1 digests
    //  of every file in the tree. The cache is saved between runs in .cache.  
    QZhash * cache ();

    //  Print contents of directory to open stream
    void fprint (FILE *file, int indent);

    //  Print contents of directory to stdout
    void print (int indent);

    //  Create a new zdir_watch actor instance:                       
    //                                                                
    //      zactor_t *watch = zactor_new (zdir_watch, NULL);          
    //                                                                
    //  Destroy zdir_watch instance:                                  
    //                                                                
    //      zactor_destroy (&watch);                                  
    //                                                                
    //  Enable verbose logging of commands and activity:              
    //                                                                
    //      zstr_send (watch, "VERBOSE");                             
    //                                                                
    //  Subscribe to changes to a directory path:                     
    //                                                                
    //      zsock_send (watch, "ss", "SUBSCRIBE", "directory_path");  
    //                                                                
    //  Unsubscribe from changes to a directory path:                 
    //                                                                
    //      zsock_send (watch, "ss", "UNSUBSCRIBE", "directory_path");
    //                                                                
    //  Receive directory changes:                                    
    //      zsock_recv (watch, "sp", &path, &patches);                
    //                                                                
    //      // Delete the received data.                              
    //      free (path);                                              
    //      zlist_destroy (&patches);                                 
    static void watch (QZsock *pipe, void *unused);

    //  Self test of this class.
    static void test (bool verbose);

    zdir_t *self;
};
#endif //  Q_ZDIR_H
/*
################################################################################
#  THIS FILE IS 100% GENERATED BY ZPROJECT; DO NOT EDIT EXCEPT EXPERIMENTALLY  #
#  Please refer to the README for information about making permanent changes.  #
################################################################################
*/
