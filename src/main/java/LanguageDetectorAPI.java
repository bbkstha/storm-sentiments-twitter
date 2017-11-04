import com.google.common.base.Optional;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.text.CommonTextObjectFactories;
import com.optimaize.langdetect.text.TextObject;
import com.optimaize.langdetect.text.TextObjectFactory;
import twitter4j.TwitterException;


import java.io.IOException;
import java.util.List;

public class LanguageDetectorAPI {

    public static void main(String[] args) throws TwitterException {

        try {
            List<LanguageProfile> languageProfiles = new LanguageProfileReader().readAllBuiltIn();
            LanguageDetector languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
                                                                        .withProfiles(languageProfiles).build();
            TextObjectFactory textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText();
            String text = "Hello";
            TextObject textObject = textObjectFactory.forText(text);
            Optional<LdLocale> lang = languageDetector.detect(textObject);
            if (lang.isPresent()) {
                System.out.println(lang.get().toString());
            } else {
                System.out.println("No language matched!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
