if [[ "$OSTYPE" == "linux-gnu" ]]; then                                             
        echo "linux"                                                                
elif [[ "$OSTYPE" == "darwin"* ]]; then                                             
        echo "mac"                                                                  
        # Mac OSX                                                                   
elif [[ "$OSTYPE" == "cygwin" ]]; then                                              
        echo "cygwin"                                                               
        # POSIX compatibility layer and Linux environment emulation for Windows     
elif [[ "$OSTYPE" == "msys" ]]; then                                                
        echo "msys"                                                                 
        # Lightweight shell and GNU utilities compiled for Windows (part of MinGW)  
elif [[ "$OSTYPE" == "win32" ]]; then                                               
        echo "win32"                                                                
        # I'm not sure this can happen.                                             
elif [[ "$OSTYPE" == "freebsd"* ]]; then                                            
        echo "freebsd"                                                              
        # ...                                                                       
else                                                                                
        echo "unknown"                                                              
        # Unknown.                                                                  
fi                                                                                  