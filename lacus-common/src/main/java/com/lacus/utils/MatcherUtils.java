package com.lacus.utils;

import com.lacus.common.exception.CustomException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class MatcherUtils {

    private static final String REG_1 = "^([hH][tT]{2}[pP]://|[hH][tT]{2}[pP][sS]://)(([A-Za-z0-9-~]+).)+([A-Za-z0-9-~\\\\/])+$";


    public static boolean isHttpsOrHttp(String url) {
        Pattern p = Pattern.compile(REG_1);
        Matcher m = p.matcher(url.trim());
        if (!m.matches()) {
            return false;
        }
        return true;
    }

    public static String lastUrlValue(String url) {
        if (StringUtils.isEmpty(url)) {
            return null;
        }
        if (!isHttpsOrHttp(url)) {
            log.error("非法的url :{}", url);
            throw new CustomException("非法的url");
        }
        String[] val = url.trim().split("/");
        return val[val.length - 1];
    }
}
