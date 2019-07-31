/*
 * Copyright (C) 2019 jumei, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.xuejunc.deerhunter.common;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * 文本处理抽象类
 * <p>
 * 子类只需实现对每行文本的处理方式
 * <p>
 * Created on 2019-04-25.
 * Copyright (c) 2019, deerhunter0837@gmail.com All Rights Reserved.
 *
 * @author Xuejunc
 */
public abstract class ITextProcessor {
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private String inputFile;
    private String outputFile;
    private boolean stop;
    protected BufferedWriter writer;

    /**
     * 构造函数
     *
     * @param inputFile  输入文件
     * @param outputFile 输出文件
     */
    public ITextProcessor(String inputFile, String outputFile) {
        this.inputFile = inputFile;
        this.outputFile = outputFile;
        try {
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), StandardCharsets.UTF_8));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void beforeProcess() throws IOException {
    }

    public void afterProcess() throws IOException {
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 依次处理输入文件的每一行
     */
    public void process() {
        try {
            beforeProcess();
            doProcess();
            afterProcess();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void doProcess() throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))
        ) {
            for (String line; !stop && (line = reader.readLine()) != null; ) {
                line = line.trim();
                // 跳过空行
                if (isNullOrEmpty(line)) {
                    continue;
                }
                try {
                    handleLine(writer, line);
                } catch (Exception e) {
                    logger.error("Error occurred.", e);
                }
            }
            writer.flush();
        }
    }

    public void stop() {
        this.stop = true;
    }

    /**
     * 处理一行文本
     * @throws Exception
     */
    protected abstract void handleLine(BufferedWriter writer, String line) throws Exception;
}