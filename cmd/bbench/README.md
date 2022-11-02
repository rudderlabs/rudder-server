# Notes

- To build both cmds, we need to adapt Makefile & Dockerfile
- Test should be performed on a production k8s namespace
- Server should be deployed as a k8s deployment with a service (use tmpDir for storing the badgerdb repo and backup)
- Client should be deployed as a k8s job, after server has prepared the backup file  (use tmpDir for storing the badgerdb repo)