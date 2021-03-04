/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.server.security;

import com.google.common.collect.ImmutableList;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.PasswordAuthenticator;
import io.trino.spi.security.PasswordAuthenticatorFactory;
import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static java.nio.file.Files.createTempFile;

public class TestPasswordAuthenticatorManager
{
    @Test
    public void testMultipleConfigFiles()
            throws Exception
    {
        Path config1 = createTempFile("TestPasswordAuthenticatorManager", "1");
        Path config2 = createTempFile("TestPasswordAuthenticatorManager", "2");
        Files.write(config1, ImmutableList.of("password-authenticator.name=type1"));
        Files.write(config2, ImmutableList.of("password-authenticator.name=type2"));

        PasswordAuthenticatorManager manager = new PasswordAuthenticatorManager(new PasswordAuthenticatorConfig()
                .setPasswordAuthenticatorFiles(config1.toAbsolutePath() + "," + config2.toAbsolutePath()));
        manager.setRequired();
        manager.addPasswordAuthenticatorFactory(new PasswordAuthenticatorFactoryMock("type1", "password1"));
        manager.addPasswordAuthenticatorFactory(new PasswordAuthenticatorFactoryMock("type2", "password2"));

        manager.loadPasswordAuthenticator();

        List<PasswordAuthenticator> authenticators = manager.getAuthenticators();
        Assertions.assertThat(login(authenticators, "password1")).isTrue();
        Assertions.assertThat(login(authenticators, "password2")).isTrue();
        Assertions.assertThat(login(authenticators, "wrong_password")).isFalse();
    }

    private boolean login(List<PasswordAuthenticator> authenticators, String password)
    {
        return authenticators.stream()
                .anyMatch(authenticator -> {
                    try {
                        authenticator.createAuthenticatedPrincipal("ignore", password);
                        return true;
                    }
                    catch (AccessDeniedException e) {
                        return false;
                    }
                });
    }

    private static class PasswordAuthenticatorFactoryMock
            implements PasswordAuthenticatorFactory
    {
        private final String name;
        private final String password;

        public PasswordAuthenticatorFactoryMock(String name, String password)
        {
            this.name = name;
            this.password = password;
        }

        @Override
        public String getName()
        {
            return name;
        }

        @Override
        public PasswordAuthenticator create(Map<String, String> config)
        {
            return (user, password) -> {
                if (password.equals(this.password)) {
                    return new BasicPrincipal("user");
                }
                throw new AccessDeniedException("You shall not pass!");
            };
        }
    }
}
