import java.io.*;
import java.util.*;


public class LogAnalyzer extends MapReduce {

    LogAnalyzer() {

    }

    public void mapper(String filePath) {
        try {
            File file = new File(filePath);
            BufferedReader in = new BufferedReader(new FileReader(file));
            String line = in.readLine();
            while(line != null) {
                this.emit_intermediate(line.split(":")[0], line.split(":")[1]);
                line = in.readLine();
            }
        } catch (Exception e) {
        }
    }

    public void reducer(String var1, LinkedList<String> var2) {
        HashSet<String> accountList = new HashSet<>();
        try {
            String accounts = "";
            for (String entry: var2) {
                for(String str: entry.split(",")) {
                    if (!accountList.contains(str)) {
                        accountList.add(str);
                        accounts += str + ",";
                    }
                }
            }
            this.emit(var1, accounts.substring(0, accounts.length() - 1));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String getIp(String var1) {
        if (var1.indexOf(" - - ") > 0) {
            return var1.split(" - - ")[0];
        } else if(var1.indexOf(" - ") > 0) {
            return var1.split(" - ")[0];
        }
        return "";
    }

    private static String getAccount(String var1, int indexOfAcc){
        String account = "";
        if (indexOfAcc >= 0) {
            int indexOfSlash = var1.indexOf("/", indexOfAcc+5);
            if (indexOfSlash >= 0) {
                account = var1.substring(indexOfAcc+6, indexOfSlash);
                if (account.contains(" ")) {
                    account = account.split(" ")[0];
                }
                if (account.contains(".")) {
                    account = "";
                }
            }
        }
        return account;
    }

    private static void processFile(File fileToProcess) throws Exception {
        String newPath = System.getProperty("user.dir") + "/processedFiles/processed_" + fileToProcess.getName();
        File f = new File(newPath);
        BufferedReader reader = new BufferedReader(new FileReader(fileToProcess));
        PrintWriter writer = new PrintWriter(new FileWriter(f));
        String ip = "", account = "";
        String line = reader.readLine();
        int accIndex;
        while(line != null) {
            accIndex = line.indexOf("GET /~");
            if (accIndex > 0) {
                ip = getIp(line);
                account = getAccount(line, accIndex);
                if (account.length() < 1) {
                    line = reader.readLine();
                    continue;
                }
                writer.println(ip + ":" + account);
            }
            line = reader.readLine();
        }
        writer.close();
    }


    private static void writeIntermediateResult(TreeMap<String, LinkedList<String>> map, File intermediateResult) throws IOException {
        PrintWriter out = new PrintWriter(new FileWriter(intermediateResult, true), true);
        String ptrStr = "";
        for (Map.Entry<String, LinkedList<String>> entry: map.entrySet()) {
            ptrStr =  entry.getKey() + ":";
            for (String str: entry.getValue()) {
                out.println(ptrStr + str);
            }
        }
        out.close();
    }

    public static void main(String[] args) {

        if (args.length == 0) {
            System.out.println("Please provide folder path as command line argument");
            System.exit(0);
        }
        try {
            String processedFilesPath = System.getProperty("user.dir") + "/processedFiles";
            File processedDir = new File(processedFilesPath);
            processedDir.mkdir();
            File[] dirFiles = new File(args[0]).listFiles();
            for (int i = 0; i < dirFiles.length; i++) {
                if (dirFiles[i].isFile() && !dirFiles[i].isDirectory()) {
                    processFile(dirFiles[i]);
                }
            }

            File[] proFiles = processedDir.listFiles();
            LogAnalyzer[] logAnalyzers = new LogAnalyzer[proFiles.length];

            for (int i = 0; i < logAnalyzers.length; i++) {
                logAnalyzers[i] = new LogAnalyzer();
                logAnalyzers[i].execute(proFiles[i].getAbsolutePath());
            }

            File intermediateResult = new File(processedDir.getAbsolutePath() + "/intermediateResult");

            for (int i = 0; i < logAnalyzers.length; i++) {
                writeIntermediateResult(logAnalyzers[i].getResults(), intermediateResult);
            }

            LogAnalyzer la2 = new LogAnalyzer();
            la2.execute(System.getProperty("user.dir") + "/processedFiles/intermediateResult");
            String outFile = System.getProperty("user.dir") + "/outputFinal.txt";
            File outputFile2 = new File(outFile);
            PrintWriter out2 = new PrintWriter(new FileWriter(outputFile2, true));

            List<Map.Entry<String, LinkedList<String>>> entryList = new ArrayList<>(la2.getResults().entrySet());

            Collections.sort(entryList, new LogComparator());
            String accountList = "";

            for (int i = entryList.size() - 1; i > entryList.size() - 16; i--) {
                out2.println("#" + (entryList.size() - i) + " occurring address: " + entryList.get(i).getKey() + "\n");
                accountList = entryList.get(i).getValue().getFirst();
                out2.println(accountList);
                out2.println("\n\n");
            }
            out2.close();
            System.out.println("Output stored in file: " + outFile);
        } catch(Exception e) {
            System.out.println("Error occured!");
        } finally {
            File fileToDelete = new File(System.getProperty("user.dir") + "/processedFiles");
            if (fileToDelete.exists()) {
                for (File e : fileToDelete.listFiles()) {
                    e.delete();
                }
                fileToDelete.delete();
            }
        }
    }
}

class LogComparator implements Comparator<Map.Entry<String, LinkedList<String>>> {

    @Override
    public int compare(Map.Entry<String, LinkedList<String>> e1, Map.Entry<String, LinkedList<String>> e2) {
        int val1 = e1.getValue().getFirst().split(",").length;
        int val2 = e2.getValue().getFirst().split(",").length;
        return val1 - val2;
    }
}
