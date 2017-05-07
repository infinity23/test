package main.java;

public class BowlingGame {

    public int getBowlingScore(String bowlingCode) {
        int res = 0;
        String s1 = bowlingCode.replaceAll("\\|", "");
        String s2 = s1.replaceAll("-", "0");
        char[] arr = s2.toCharArray();


        for (int i = 0; i < arr.length; i++) {
            if (arr[i] == 'X') {
                res += 10 + getValue(arr,i+1) + getValue(arr,i+2);
                if(i == arr.length - 3){
                    break;
                }
            }else if(arr[i] == '/'){
                res += getValue(arr,i) + getValue(arr,i+1);
                if(i == arr.length - 2){
                    break;
                }
            }else{
                res += getValue(arr,i);
            }
         }

        return res;
    }

    private int getValue(char[] arr, int index){
        if(arr[index] == 'X'){
            return 10;
        }else if(arr[index] == '/'){
            return 10 - Character.getNumericValue(arr[index - 1]);
        }else {
            return Character.getNumericValue(arr[index]);
        }
    }
}
