# zone-volume-cleaner
(This project was built from the [Statcan Volume Cleaner](https://github.com/StatCan/volume-cleaner))

A Golang script for the Zone to automatically identify and clean up stale Persistent Volume Claims(PVCs) in Kubernetes, which will cascade into deleting their bound Persistent Volumes that are linked to Azure Disks for persistent storage.

When PVCs are associated with a Notebook resource, the script will only delete the notebook and its volumes if they are all stale. 
