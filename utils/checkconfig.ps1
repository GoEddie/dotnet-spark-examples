
Function Get-Env {
    param([string]$variable)

    $searchPath = "ENV:\$variable"

    if (Test-Path $searchPath) {
        (Get-Item $searchPath).Value 
    }
}

Function Test-IsWindows {

    if ($isWindows) {
        #powershell core has a variable already
        $isWindows
    } {
        #older versions don't
        Get-Env -variable 'OS' -eq 'Windows_NT'
    }   
}

Function Test-EnvVariablesExist {

    if (-not($javaHome)) {
        Write-Host @"
        
        JAVA_HOME is not set - you need a java 1.8 or 8 runtime and JAVA_HOME should point to that.
    
        You can download either the OpenJDK from: https://adoptopenjdk.net/
        
        or the Oracle JDK 1.8 but you will need to register: https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html
        
        I would suggest you install a 64-bit version of the runtime otherwise you will struggle with memory.
    
        Once you have installed Java you need to create an environment variable called JAVA_HOME pointing to the installation directory.
        
        For example if you have installed java to "C:\Program Files\Java\jdk1.8.0_211" and this folder contains the bin folder and java.exe is inside that bin folder then set JAVA_HOME to C:\Program Files\Java\jdk1.8.0_211
"@
        $haveFailure = $true
    }
    
    if (-not($sparkHome)) {
        Write-Host "SPARK_HOME is not set - download spark from (http://spark.apache.org/downloads.html) extract it to a folder and set SPARK_HOME to that folder"
        $haveFailure = $true
    }
    
    if (-not($hadoopHome)) {
        Write-Host "HADOOP_HOME is not set - you need to download winutils.exe and put it in a folder called bin within another folder and set HADOOP_HOME to that parent folder"
        $haveFailure = $true
    }
    
    if (-not($workerPath)) {
        Write-Host "DOTNET_WORKER_DIR is not set - you might not be able to run dotnet UDF's"
    }

    $haveFailure

}

Function Test-ErrantSemiColons {

    if ($javaHome -and $javaHome.contains(';')) {
        Write-Host "JAVA_HOME should never contain a semi-colon (;) - typically the PATH or CLASS_PATH use semi-colons but if JAVA_HOME has one then it breaks everything, even if it is just sitting at the end."
        $haveFailure = $true
    }
    
    
    if ($sparkHome -and $sparkHome.contains(';')) {
        Write-Host "SPARK_HOME should never contain a semi-colon (;) - typically the PATH or CLASS_PATH use semi-colons but if SPARK_HOME has one then it breaks everything, even if it is just sitting at the end."
        $haveFailure = $true
    }
    
    
    if ($hadoopHome -and $hadoopHome.contains(';')) {
        Write-Host "HADOOP_HOME should never contain a semi-colon (;) - typically the PATH or CLASS_PATH use semi-colons but if HADOOP_HOME has one then it breaks everything, even if it is just sitting at the end."
        $haveFailure = $true
    }
    
    
    if ($workerPath -and $workerPath.contains(';')) {
        Write-Host "DOTNET_WORKER_DIR should never contain a semi-colon (;) - typically the PATH or CLASS_PATH use semi-colons but if DOTNET_WORKER_DIR has one then it breaks everything, even if it is just sitting at the end."
        $haveFailure = $true
    }   

    $haveFailure
}

Function Test-PathsPointToActualContent {
    
    
    $haveFailure = $false

    if ($hadoopHome -and $isOSWindows) {

        $path = (Join-Path (Join-Path $hadoopHome 'bin') 'winutils.exe')

        if (-not(Test-Path $path)) {
            Write-Host @"
    
    HADOOP_HOME is set BUT you also need to download winutils.exe (https://github.com/steveloughran/winutils/releases) and put it in a folder called bin within another folder and set HADOOP_HOME to that parent folder
    
    Your HADOOP_HOME is set to '$($hadoopHome)' so winutils should be: '$($path)'
"@

            $haveFailure = $true

        }
    }

    if ($javaHome) {
    
        $path = (Join-Path (Join-Path $javaHome 'bin') 'java.exe')
        if (-not(Test-Path $path)) {
            Write-Host "Could not find java.exe in your JAVA_HOME path? Tested path = '$($path)'"
            $haveFailure = $true
        }
    }   

    if($sparkHome){
        $path = (Join-Path (Join-Path $sparkHome 'bin') 'spark-submit')
        if (-not(Test-Path $path)) {
            Write-Host "Could not find spark-submit in your SPARK_HOME path? Tested path = '$($path)', it looks like SPARK_HOME isn't pointing to the correct folder"
            $haveFailure = $true
        }
    }

    if($workerPath){

        $path = (Join-Path $workerPath 'Microsoft.Spark.Worker.exe')
        if (-not(Test-Path $path)) {
            Write-Host "Could not find Microsoft.Spark.Worker.exe in your DOTNET_WORKER_DIR path? Tested path = '$($path)'"
            $haveFailure = $true
        }
        
    }

    $haveFailure
}

Function Test-PathContainsBinPaths{

    Function Any{ 
        begin { 
            $any = $False
        } 
        process {
            $any = $true
        } 
        end {
            $any
        }
    }

    $haveFailure = $false

    if(-not(($ENV:Path).Split(';') | ?{$_ -ne '' -and (Test-Path (Join-Path $_ 'spark-submit'))} |Any)){
        Write-Host "Could not find spark-submit on your path, try adding '%SPARK_HOME%\bin' to your path"
        $haveFailure = $true    
    }

    if(-not(($ENV:Path).Split(';') | ?{$_ -ne '' -and (Test-Path (Join-Path $_ 'java.exe'))} |Any)){
        Write-Host "Could not find java on your path, try adding '%JAVA_HOME%\bin' to your path"
        $haveFailure = $true
    }

    $haveFailure

}

$javaHome = (Get-Env JAVA_HOME)
$sparkHome = (Get-Env SPARK_HOME)
$hadoopHome = (Get-Env HADOOP_HOME)
$workerPath = (Get-Env DOTNET_WORKER_DIR)

$isOSWindows = Test-IsWindows 
$haveFailure = $false

$haveFailure = $haveFailure -or (Test-EnvVariablesExist)
$haveFailure = $haveFailure -or (Test-ErrantSemiColons)
$haveFailure = $haveFailure -or (Test-PathsPointToActualContent)
$haveFailure = $haveFailure -or (Test-PathContainsBinPaths)

if(-not($haveFailure)){
    Write-Host 'It certainly looks good, lets run a test job...'
    Write-Host 'You should see some action and "Pi is roughly 3.142355711778559", if you do then it is all good'
    Write-Host 'Press enter to run the test...'
    Read-Host
    run-example SparkPi
}